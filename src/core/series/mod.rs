use std::collections::HashMap;

use arrow2::{
    datatypes::{DataType, Schema, Metadata},
    array::{Array, PrimitiveArray, ArrayValuesIter},
    chunk::Chunk,
    compute::hash::hash,
    compute::concatenate::concatenate,
};

pub struct Series {
    array: PrimitiveArray
};

impl Series {

    pub fn new(array: <dyn Array>) -> PrimitiveArray<T> {
        println!("{:#?}", schema.fields);
        Series { array }
    }

    pub fn into_iter(&self) {
        match array.DataType {
            DataType::Int64 => 
        }

    }

}