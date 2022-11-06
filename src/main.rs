use std::time::SystemTime;

mod core;
mod io;

use io::parquet::read::read_parquet;
use crate::core::dataset::{Dataset, Format, Compression, DatasetStorage};

// TODO LIST
// 1. Table: append, upsert, delete 
// 2. Groupby: multiple columns + buckets
// 3. Dataset naming / bucketing conventions
// 4. Delete records in folder when writing partitions

fn main() {
    let start = SystemTime::now();
    let table = read_parquet("data/skus/org_key=1/file.parquet").expect("Reading parquet failed!");
    let _columns = table.columns();
    let _rows = table.num_rows();
    println!("Reading table took: {} ms", start.elapsed().unwrap().as_millis());

    let store = DatasetStorage::new("data/skus_parts".to_string(), Format::Parquet, Some(Compression::Snappy));

    let start = SystemTime::now();
    let partitions = vec!["sku_key".to_string()];
    let dataset = table.merge(&table, &partitions);
    println!("Merge sku_key took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let partitions = vec!["group_key".to_string()];
    let dataset = table.groupby(&partitions);
    println!("Groupby single took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let dataset = table.groupby_test(&partitions);
    println!("Groupby single V2 took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let dataset = table.groupby(&partitions);
    println!("Groupby single took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let partitions = vec!["group_key".to_string(), "collection_key".to_string()];
    let dataset = table.groupby(&partitions);
    println!("Groupby multiple took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let dataset = table.groupby_test(&partitions);
    println!("Groupby multiple V2 took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let dataset = table.to_dataset(Some(partitions), None, Some(store));
    println!("To dataset: {} ms", start.elapsed().unwrap().as_millis());
    
    let start = SystemTime::now();
    dataset.to_storage();
    println!("Writing to storage: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();    
    let lazy: bool = true;
    let dataset2 = Dataset::from_storage(&"data/skus_parts".to_string(), lazy);
    println!("Reading from storage (lazy): {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();    
    let dataset2 = Dataset::from_storage(&"data/skus_parts".to_string(), !lazy);
    println!("Reading from storage (!lazy): {} ms", start.elapsed().unwrap().as_millis());
}