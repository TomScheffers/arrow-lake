use std::collections::HashMap;
use rayon::prelude::*;
use std::time::SystemTime;

use arrow2::{
    datatypes::Field,
    array::{Array, PrimitiveArray},
    chunk::Chunk,
    compute::concatenate::concatenate,
    compute::take::take,
};

use crate::core::hm::{hashmap_to_kv, hashmap_primitive_to_idxs_par, hashmap_from_vecs, hashmaps_merge};
use crate::core::groupby::{groupby_many, groupby_many_test};
use crate::core::chunks::{chunk_take, chunk_head};
use crate::core::dataset::{DatasetPart, Dataset, DatasetStorage};
use crate::core::join::{join_arrays};
use crate::core::merge::{merge_arrays, delete_arrays};
use crate::io::parquet::write::write_parquet;

#[derive(Clone)]
pub struct Table {
    pub fields: Vec<Field>,
    pub chunks: Vec<Chunk<Box<dyn Array>>>,
}

impl Table {
    pub fn new(fields: Vec<Field>, chunks: Vec<Chunk<Box<dyn Array>>>) -> Table {
        Table { fields, chunks }
    }

    pub fn columns(&self) -> Vec<&String> {
        self.fields.iter().map(|f| &f.name).collect()
    }

    pub fn num_rows(&self) -> usize {
        self.chunks.iter().map(|c| c.len()).sum()
    }
    pub fn len(&self) -> usize {self.num_rows()}

    pub fn head(&self, n: &usize) -> Table {
        let mut remaining = n.clone();
        let mut new_chunks = Vec::new();
        for chunk in &self.chunks {
            if chunk.len() > remaining {
                new_chunks.push(chunk_head(&chunk, &remaining));
                remaining = 0
            } else {
                new_chunks.push(chunk.clone());
                remaining -= &chunk.len();
            }
            if remaining == 0 {break};
        }
        Self { fields:self.fields.clone(), chunks:new_chunks }
    }

    pub fn table_eq(&self, other: &Table) {
        assert_eq!(self.fields, other.fields);
    }

    pub fn position(&self, column: &String) -> usize {
        self.columns().iter().position(|&r| r == column).unwrap()
    }
 
    pub fn column(&self, column: &String) -> Box<dyn Array> {
        let idx = self.position(column);
        let arrays = self.chunks
            .iter()
            .map(|chunk| {
                (*chunk.columns().get(idx).unwrap()).as_ref()
            })
            .collect::<Vec<&dyn Array>>();
        concatenate(&arrays[..]).unwrap()
    }

    pub fn take(&self, idxs: Vec<u32>) -> Self {
        let idx = PrimitiveArray::from(idxs.iter().map(|x| Some(*x)).collect::<Vec<Option<u32>>>());
        let arrays = (0..self.fields.len())
            .into_par_iter()
            .map(|i| {
                if self.chunks.len() == 1 {
                    take(self.chunks[0].columns()[i].as_ref(), &idx).unwrap()
                } else {
                    let arrs = self.chunks
                        .iter()
                        .map(|chunk| (*chunk.columns().get(i).unwrap()).as_ref())
                        .collect::<Vec<&dyn Array>>();
                    take(concatenate(&arrs).unwrap().as_ref(), &idx).unwrap()
                }
            })
            .collect::<Vec<Box<dyn Array>>>();
        Self { fields: self.fields.clone(), chunks: vec![Chunk::new(arrays)] }
    }
    
    pub fn append(&mut self, other: &mut Table) {
        self.table_eq(other);
        self.chunks.append(&mut other.chunks);
    }

    pub fn upsert(&self, other: &Table, columns: &Vec<String>) -> Self {
        self.table_eq(other);

        // Gather arrays of both tables
        let left = columns.iter().map(|col| self.column(col)).collect::<Vec<Box<dyn Array>>>();
        let right = columns.iter().map(|col| other.column(col)).collect::<Vec<Box<dyn Array>>>();

        // Merge to idxs
        let (left_idxs, right_idxs) = merge_arrays(&left, &right);

        // Index left & right + concatenate tables
        let mut lt = self.take(left_idxs);
        let mut rt = other.take(right_idxs);
        lt.append(&mut rt);
        lt
    }

    pub fn delete(&self, other: &Table, columns: &Vec<String>) -> Self {
        self.table_eq(other);
        let left = columns.iter().map(|col| self.column(col)).collect::<Vec<Box<dyn Array>>>();
        let right = columns.iter().map(|col| other.column(col)).collect::<Vec<Box<dyn Array>>>();

        // Merge to idxs
        let left_idxs = delete_arrays(&left, &right);

        // Index left idxs
        self.take(left_idxs)
    }
    
    pub fn join(&self, other: &Table, columns: &Vec<String>) {
        // Gather arrays of both tables
        let arrays1 = columns.iter().map(|col| self.column(col)).collect::<Vec<Box<dyn Array>>>();
        let arrays2 = columns.iter().map(|col| other.column(col)).collect::<Vec<Box<dyn Array>>>();

        // Join to idxs
        join_arrays(&arrays1, &arrays2);
    }

    pub fn groupby_test(&self, columns: &Vec<String>) {
        let maps = self.chunks
            .par_iter()
            .map(|chunk| {
                let arrays = columns
                    .iter()
                    .map(|column| {
                        let idx = self.position(&column);
                        chunk.columns().get(idx).unwrap().as_ref()                        
                    })
                    .collect::<Vec<&dyn Array>>();
                
                let map = groupby_many_test(arrays);
            })
            .collect::<Vec<()>>();
    }

    pub fn groupby(&self, columns: &Vec<String>) -> Vec<DatasetPart> {
        // 1. For loop over row_groups / chunks
        // 2. Create hash grouping within row_groups
        // 3. Groupby for each row_group
        // 4. Gather results per key

        let maps = self.chunks
            .par_iter()
            .map(|chunk| {
                let arrays = columns
                    .iter()
                    .map(|column| {
                        let idx = self.position(&column);
                        chunk.columns().get(idx).unwrap().as_ref()                        
                    })
                    .collect::<Vec<&dyn Array>>();
                
                let map = groupby_many(arrays);
                map
                    .into_par_iter()
                    .map(|(k, v)| {
                        (k, chunk_take(&chunk, &v))
                    })
                    .collect::<HashMap<Vec<String>, Chunk<Box<dyn Array>>>>()

            })
            .collect::<Vec<HashMap<Vec<String>, Chunk<Box<dyn Array>>>>>();

        let tables = hashmaps_merge(maps)
            .par_iter()
            .map(|(k, v)| {
                let filters = columns.clone().into_iter().zip(k.into_iter().map(|kr| kr.clone())).collect::<HashMap<String, String>>();
                let table = Table { fields:self.fields.clone(), chunks: v.clone() };
                DatasetPart::new(Some(table), Some(filters), None)
            })
            .collect::<Vec<DatasetPart>>();
        tables
    }

    pub fn to_dataset(&self, partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, storage: Option<DatasetStorage>) -> Dataset {
        let parts = match &partitions {
            Some(partitions) => self.groupby(&partitions),
            None => {
                vec![DatasetPart::new(Some(self.clone()), Some(HashMap::<String, String>::new()), None)]
            }
        };
        Dataset::new(partitions.clone(), buckets.clone(), parts, storage)
    }

    // IO RELATED
    pub fn to_parquet(&self, path: &String) {
        write_parquet(path, self.fields.clone().into(), &self.chunks).expect("Writing Table to parquet failed");
    }

}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::factory::create_random_table;

    #[test]
    fn test_append() {
        let mut t1 = create_random_table(2);
        let mut t2 = create_random_table(1);
        let len = &t1.num_rows() + &t2.num_rows();
        t1.append(&mut t2);
        assert_eq!(&t1.num_rows(), &len);
    }
}