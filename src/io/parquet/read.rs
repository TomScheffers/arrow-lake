use std::fs::File;
use rayon::prelude::*;

use arrow2::{
    array::Array,
    chunk::Chunk,
    error::Result,
    io::parquet::read::{self, ArrayIter},
};

use crate::core::table::Table;

fn deserialize_parallel(iters: &mut [ArrayIter<'static>]) -> Result<Chunk<Box<dyn Array>>> {
    // CPU-bounded
    let arrays = iters
        .par_iter_mut()
        .map(|iter| iter.next().transpose())
        .collect::<Result<Vec<_>>>()?;

    Chunk::try_new(arrays.into_iter().map(|x| x.unwrap()).collect())
}

pub fn read_parquet(path: &str) -> Result<Table> {
    // open the file
    let mut reader = File::open(path)?;

    // read Parquet's metadata and infer Arrow schema
    let metadata = read::read_metadata(&mut reader)?;
    let schema = read::infer_schema(&metadata)?;

    // we can read the statistics of all parquet's row groups (here for each field)
    // for field in &schema.fields {
    //     let statistics = read::statistics::deserialize(field, &metadata.row_groups)?;
    //     println!("{:#?}, {:#?}", field, statistics);
    // }

    let chunks = metadata.row_groups
        .par_iter()
        .map(|rg| {
            let schema2 = schema.clone();
            let mut reader2 = File::open(path).unwrap();
            let mut columns = read::read_columns_many(&mut reader2, rg, schema2.fields, Some(1024 * 8 * 8), None, None).unwrap();        
            deserialize_parallel(&mut columns).unwrap()
        })
        .collect::<Vec<_>>();

    // create table
    let table = Table::new( schema.fields, chunks );

    Ok(table)
}