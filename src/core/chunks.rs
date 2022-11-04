use std::collections::HashMap;
use rayon::prelude::*;

use arrow2::{
    datatypes::Field,
    array::{Array, PrimitiveArray},
    chunk::Chunk,
    compute::concatenate::concatenate,
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