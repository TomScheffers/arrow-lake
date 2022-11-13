use arrow2::{
    array::*,
    chunk::Chunk,
    datatypes::*,
};

use crate::core::table::Table;

fn create_chunk(size: usize, offset: usize) -> Chunk<Box<dyn Array>> {
    let c1: Int32Array = (offset..offset+size)
        .map(|x| Some((x % 10) as i32))
        .collect();
    let c2: Int32Array = (offset..offset+size)
        .map(|x| Some(x as i32))
        .collect();
    let c3: Utf8Array<i64> = (offset..offset+size)
        .map(|x| Some(x.to_string()))
        .collect();
    let c4: Int64Array = (offset..offset+size)
        .map(|x| Some(x as i64))
        .collect();
    
    Chunk::new(vec![
        c1.clone().boxed(),
        c2.clone().boxed(),
        c3.boxed(),
        c4.boxed(),
    ])
}

pub fn create_random_table(size: usize) -> Table {
    let fields = vec![
        Field::new("c1", DataType::Int32, true),
        Field::new("c2", DataType::Int32, true),
        Field::new("c3", DataType::LargeUtf8, true),
        Field::new("c4", DataType::Int64, true),
    ];
    let mut chunks = Vec::new();
    let rows = 100_000;
    for i in 0..size {
        chunks.push(create_chunk(rows, i * rows));
    }

    Table::new(fields, chunks)
}