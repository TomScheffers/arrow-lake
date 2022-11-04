use std::path::Path;
use std::fs::{self, DirEntry};
use std::collections::HashMap;

use serde_json;
use serde::{Serialize, Deserialize};

use arrow2::datatypes::Schema;
use rayon::prelude::*;

use crate::core::table::Table;
use crate::io::parquet::read::read_parquet;

#[derive(Serialize, Deserialize, Debug)]
pub enum Format {
    Parquet,
    Ipc,
    Csv,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Compression {
    Snappy,
    // Gzip,
    // Lzo,
    // Brotli,
    // Lz4,
    // Zstd,
    Lz4Raw,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DatasetStorage {
    root: String, // Root folder (relative to code)
    format: Format,
    compression: Option<Compression>,
    // versioned: Bool,
    // acid: Option<Bool>
}

impl DatasetStorage {
    pub fn new( root: String, format: Format, compression: Option<Compression>) -> Self {
        Self { root, format, compression }
    }
}

pub struct DatasetPart {
    table: Option<Table>, // For lazy loading
    filters: Option<HashMap<String, String>>,
    path: Option<String>,
}

impl DatasetPart {
    pub fn new( table: Option<Table>, filters: Option<HashMap<String, String>>, path: Option<String>) -> Self {
        Self { table, filters, path }
    }

    pub fn partition_path(&self, partitions: &Option<Vec<String>>) -> String {
        match &partitions {
            Some(partitions) => {
                let mut parts = Vec::new();
                for p in partitions {
                    let v = self.filters.as_ref().unwrap().get(p).unwrap();
                    parts.push(format!("{}={}", p, v));
                }
                parts.join("/")
            },
            None => "".to_string()
        }
    }

    pub fn load(&mut self) {
        match &self.path {
            Some(path) => {self.table = Some(read_parquet(path).unwrap());},
            None => println!("Path was not specified!")
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Dataset {
    pub partitions: Option<Vec<String>>, // File based partitioning columns
    pub buckets: Option<Vec<String>>, // Hash bucketing columns (within partitions)
    #[serde(skip_serializing, skip_deserializing)]
    pub parts: Vec<DatasetPart>, // Underlying parts (referencing to tables)
    pub storage: Option<DatasetStorage> // Storage options
}

fn extract_files<'a>(dir: &Path, contains: &String, files: &'a mut Vec<String>) -> &'a mut Vec<String> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                extract_files(&path, contains, files);
            } else {
                let file = path.to_str().unwrap().to_string();
                if file.contains(contains) {
                    files.push(file);
                }
            }
        }
    }
    files
}

fn find_parts(root: &String, contains: &String, lazy: bool) -> Vec<DatasetPart> {
    let mut empty = Vec::new();
    let root_files = extract_files(&Path::new(root), contains, &mut empty);

    let parts = root_files 
        .par_iter()
        .map(|path| {
            let mut filters = HashMap::new();
            for v in path.split("/").collect::<Vec<&str>>() {
                if v.contains("=") {
                    let arr = v.split("=").collect::<Vec<&str>>();
                    filters.insert(arr[0].to_string(), arr[1].to_string());
                }
            }
            let mut part = DatasetPart::new(None, Some(filters), Some(path.clone()));
            if !lazy {part.load()};
            part
        })
        .collect::<Vec<DatasetPart>>();
    parts
}

impl Dataset {
    // CREATION
    pub fn new(partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, parts: Vec<DatasetPart>, storage: Option<DatasetStorage>) -> Self {
        Self { partitions, buckets, parts, storage }
    }

    // Utils
    pub fn is_partitioned(&self) -> bool {self.partitions.is_some()}
    pub fn is_bucketized(&self) -> bool {self.buckets.is_some()}

    // TABLE INTERACTIONS
    // pub fn append(&self, table: &Table) {}
    // pub fn upsert(&self, table: &Table) {}
    // pub fn delete(&self, table: &Table) {}
    
    // IO RELATED

    pub fn from_storage(root: &String, lazy: bool) -> Self {
        let fpath = format!("{root}/manifest.json");
        let contents = std::fs::read_to_string(&fpath).expect("Could not read manifest file in given root");
        let mut obj = serde_json::from_str::<Self>(&contents).expect("Issue in deserialization of manifest");

        // Lazy load underlying parts
        let contains = "parquet".to_string();
        obj.parts = find_parts(&obj.storage.as_ref().unwrap().root, &contains, lazy);

        obj
    }

    pub fn to_storage(&self) {
        match &self.storage {
            Some(storage) => {
                // Create & clear directory
                fs::remove_dir_all(&storage.root).expect("Remove files in root dir failed");
                fs::create_dir(&storage.root).expect("Create dir failed");

                // Save manifest
                let path = format!("{}/manifest.json", storage.root);
                fs::write(
                    path,
                    serde_json::to_string_pretty(&self).expect("Issue in serialization of manifest"),
                )
                .unwrap();

                // Save underlying parts
                self.parts
                    .par_iter()
                    .map(|p| {
                        let ppath = p.partition_path(&self.partitions);
                        match &p.table {
                            Some(t) => {
                                // println!("Writing to storage {} with rows: {}", ppath, t.num_rows());
                                let fpath = Path::new(&storage.root).join(ppath).join("file.parquet").to_str().expect("Path merging failed").to_string();
                                t.to_parquet(&fpath);
                            }
                            None => println!("Table has not been loaded yet!")
                        }
                    })
                    .collect::<()>();
            },
            None => println!("Storage options are not set on dataset")
        }
    }
}

