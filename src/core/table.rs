use std::collections::HashMap;
use rayon::prelude::*;

use arrow2::{
    datatypes::Field,
    array::{Array, PrimitiveArray},
    chunk::Chunk,
    compute::concatenate::concatenate,
    compute::take::take,
};

use crate::core::hm::{hashmap_to_kv, hashmap_primitive_to_idxs_par, hashmap_from_vecs, hashmaps_merge};
use crate::core::groupby::{groupby_many, groupby_many_test};
use crate::core::chunks::{chunk_take_idxs};
use crate::core::dataset::{DatasetPart, Dataset, DatasetStorage};
use crate::core::join::{join_arrays};
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

    pub fn position(&self, column: &String) -> usize {
        self.columns().iter().position(|&r| r == column).unwrap()
    }
 
    pub fn table_column(&self, column: &String) -> Box<dyn Array> {
        let idx = self.position(column);
        let arrays = self.chunks
            .iter()
            .map(|chunk| {
                (*chunk.columns().get(idx).unwrap()).as_ref()
            })
            .collect::<Vec<_>>();
        concatenate(&arrays[..]).unwrap()
    }
    
    pub fn append(&mut self, other: &mut Table) {
        assert_eq!(self.fields, other.fields);
        self.chunks.append(&mut other.chunks);
    }

    pub fn upsert(&mut self, other: &mut Table) {}

    pub fn delete(&mut self, other: &mut Table) {}
    
    pub fn join(&self, other: &Table, columns: &Vec<String>) {
        // TODO: Push down filters to other table

        // Gather arrays of both tables
        let arrays1 = columns.iter().map(|col| self.table_column(col)).collect::<Vec<Box<dyn Array>>>();
        let arrays2 = columns.iter().map(|col| other.table_column(col)).collect::<Vec<Box<dyn Array>>>();

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
                        (k, chunk_take_idxs(&chunk, &v))
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