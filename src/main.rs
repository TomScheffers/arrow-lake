#![allow(dead_code)]

use std::time::SystemTime;

mod core;
mod io;

use io::parquet::read::read_parquet;
use crate::core::dataset::{Dataset, Format, Compression, DatasetStorage};

// TODO LIST
// 1. Table: append, upsert, delete 
// 2. Filter ops
// 3. Large benchmark (millions of records)
// 4. Dataset: bucketing: naming / conventions
// 5. Printing a table head

fn main() {
    let start = SystemTime::now();
    let table = read_parquet("data/skus/org_key=1/file.parquet").expect("Reading parquet failed!");
    let _columns = table.columns();
    let _rows = table.num_rows();
    println!("Reading table took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let upsert = table.head(&10_000);
    let partitions = vec!["sku_key".to_string()];
    let table_up = table.upsert(&upsert, &partitions);
    println!("Upsert on sku_key took: {} ms. Rows: {}", start.elapsed().unwrap().as_millis(), table_up.len());

    let start = SystemTime::now();
    let delete = table.head(&1_000);
    let table_del = table.delete(&delete, &partitions);
    println!("Delete on sku_key took: {} ms. Rows: {}", start.elapsed().unwrap().as_millis(), table_del.len());

    let start = SystemTime::now();
    let partitions = vec!["group_key".to_string()];
    let _ = table.groupby(&partitions);
    println!("Groupby single took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let partitions = vec!["group_key".to_string(), "collection_key".to_string()];
    let _ = table.groupby(&partitions);
    println!("Groupby multiple took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let store = DatasetStorage::new("data/skus_parts".to_string(), Format::Parquet, Some(Compression::Snappy));
    let dataset = table.to_dataset(Some(partitions), None, Some(store));
    println!("To dataset: {} ms", start.elapsed().unwrap().as_millis());
    
    let start = SystemTime::now();
    dataset.to_storage();
    println!("Writing to storage: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();    
    let lazy: bool = true;
    let _ = Dataset::from_storage(&"data/skus_parts".to_string(), lazy);
    println!("Reading from storage (lazy): {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();    
    let _ = Dataset::from_storage(&"data/skus_parts".to_string(), !lazy);
    println!("Reading from storage (!lazy): {} ms", start.elapsed().unwrap().as_millis());
}