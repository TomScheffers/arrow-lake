//! Example demonstrating how to write to parquet in parallel.
use std::collections::VecDeque;

use rayon::prelude::*;

use arrow2::{
    array::*,
    chunk::Chunk,
    datatypes::*,
    error::{Error, Result},
    io::parquet::{read::ParquetError, write::*},
};

struct Bla {
    columns: VecDeque<CompressedPage>,
    current: Option<CompressedPage>,
}

impl Bla {
    pub fn new(columns: VecDeque<CompressedPage>) -> Self {
        Self {
            columns,
            current: None,
        }
    }
}

impl FallibleStreamingIterator for Bla {
    type Item = CompressedPage;
    type Error = Error;

    fn advance(&mut self) -> Result<()> {
        self.current = self.columns.pop_front();
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

pub fn write_parquet(path: &str, schema: Schema, chunks: &Vec<Chunk<Box<dyn Array>>>) -> Result<()> {
    // declare the options
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Lz4Raw,
        version: Version::V2,
    };

    let encoding_map = |data_type: &DataType| {
        match data_type.to_physical_type() {
            // remaining is plain
            _ => Encoding::Plain,
        }
    };

    // declare encodings
    let encodings = (&schema.fields)
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect::<Vec<_>>();

    // derive the parquet schema (physical types) from arrow's schema.
    let parquet_schema = to_parquet_schema(&schema)?;

    let row_groups = chunks.iter().map(|chunk| {
        // write batch to pages; parallelized by rayon
        let columns = chunk
            .columns()
            .par_iter()
            .zip(parquet_schema.fields().to_vec())
            .zip(encodings.par_iter())
            .flat_map(move |((array, type_), encoding)| {
                let encoded_columns = array_to_columns(array, type_, options, encoding).unwrap();
                encoded_columns
                    .into_iter()
                    .map(|encoded_pages| {
                        let encoded_pages = DynIter::new(
                            encoded_pages
                                .into_iter()
                                .map(|x| x.map_err(|e| ParquetError::FeatureNotSupported(e.to_string()))),
                        );
                        encoded_pages
                            .map(|page| {
                                compress(page?, vec![], options.compression).map_err(|x| x.into())
                            })
                            .collect::<Result<VecDeque<_>>>()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Result<Vec<VecDeque<CompressedPage>>>>()?;

        let row_group = DynIter::new(
            columns
                .into_iter()
                .map(|column| Ok(DynStreamingIterator::new(Bla::new(column)))),
        );
        Result::Ok(row_group)
    });

    // Create a new empty file
    let path_dir = std::path::Path::new(path).parent().unwrap();
    std::fs::create_dir_all(path_dir).expect("Create dir failed");
    let file = std::io::BufWriter::new(std::fs::File::create(path).expect("Create file failed"));

    let mut writer = FileWriter::try_new(file, schema, options)?;

    // Write the file.
    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;

    Ok(())
}