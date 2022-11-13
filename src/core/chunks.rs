use std::cmp::min;

use arrow2::{
    array::{Array, PrimitiveArray},
    chunk::Chunk,
    compute::take::take,
};

pub fn chunk_take_idxs(chunk: &Chunk<Box<dyn Array>>, idxs: &Vec<u32>) -> Chunk<Box<dyn Array>> {
    let mut arrays_new = Vec::new();
    let idxs = PrimitiveArray::from(idxs.iter().map(|x| Some(*x)).collect::<Vec<Option<u32>>>());
    for array in chunk.columns() {
        arrays_new.push(take(array.as_ref(), &idxs).unwrap());
    }
    Chunk::new(arrays_new)
}

pub fn chunk_head(chunk: &Chunk<Box<dyn Array>>, n: &usize) -> Chunk<Box<dyn Array>> {
    let mut arrays_new = Vec::new();
    for array in chunk.columns() {
        arrays_new.push(array.slice(0, min(array.len(), *n)));
    }
    Chunk::new(arrays_new)
}