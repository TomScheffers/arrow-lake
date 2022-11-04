use std::time::SystemTime;

use std::collections::{HashSet, HashMap};
use std::hash::Hash;
use std::cmp::Eq;

use rayon::prelude::*;

use arrow2::{
    types::NativeType,
    datatypes::*,
    array::{Array, PrimitiveArray, DictionaryArray, DictionaryKey},
    compute::take::take,
    compute::cast::{primitive_to_primitive, primitive_to_dictionary},
    compute::arithmetics::basic::{add, add_scalar, mul, mul_scalar},
    compute::aggregate::{min_primitive, max_primitive},
    compute::hash::hash,
    compute::sort::{SortOptions, sort_to_indices},
    error::Error,
};

use crate::core::hm2::{hashmap_to_kv, hashmap_primitive_to_idxs_par, hashmap_from_vecs};

fn array_to_idxs(array: &dyn Array) -> Result<HashMap<Option<i64>, Vec<u32>>, String> {
    match array.data_type() {
        // DataType::Int8   => Ok(hashmap_primitive_to_idxs_par::<i8 >(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        // DataType::Int16  => Ok(hashmap_primitive_to_idxs_par::<i16>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        // DataType::Int32  => Ok(hashmap_primitive_to_idxs_par::<i32>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::Int64  => Ok(hashmap_primitive_to_idxs_par::<i64>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),         
        // DataType::UInt8  => Ok(hashmap_primitive_to_idxs_par::<u8 >(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        // DataType::UInt16 => Ok(hashmap_primitive_to_idxs_par::<u16>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        // DataType::UInt32 => Ok(hashmap_primitive_to_idxs_par::<u32>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        // DataType::UInt64 => Ok(hashmap_primitive_to_idxs_par::<u64>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),         
        _ => Err(format!("{:?} is not implemented for hashing", array.data_type()))
    }    
}

fn array_downcast_primitive<T: NativeType + Eq>(array: &dyn Array) -> &PrimitiveArray<T> {
    array.as_any().downcast_ref::<PrimitiveArray<T>>().expect("Downcast to primitive failed")
}

fn array_to_u64(array: &dyn Array) -> PrimitiveArray<u64> {
    let arr = match array.data_type() {
        DataType::Int64  => Ok(array_downcast_primitive::<i64>(array)),               
        _ => Err(format!("{:?} is not implemented for hashing", array.data_type()))
    }; 
    let arr = arr.unwrap(); 
    let min_value = min_primitive(arr).unwrap();
    let parr = add_scalar(arr, &min_value);
    primitive_to_primitive(&parr, &DataType::UInt64)
}

fn arrays_to_hash(arrays: &Vec<Box<dyn Array>>) -> PrimitiveArray<u64> {
    let int_arrays = arrays.into_iter().map(|array| array_to_u64(array.as_ref())).collect::<Vec<PrimitiveArray<u64>>>();
    let mut uidxs = int_arrays[0].clone();
    let mut max_last = max_primitive(&uidxs).unwrap();
    for iarr in int_arrays[1..].into_iter() {
        uidxs = mul_scalar(&uidxs, &max_last); // val2 = hash1 * max1 + hash2
        uidxs = add(&uidxs, iarr);
        max_last = max_primitive(&uidxs).unwrap();
    }
    uidxs
}

fn join_probe(probe: &PrimitiveArray<i64>, build_map: &HashMap<Option<i64>, Vec<u32>>) {
    let bidxs = probe
        .iter()
        .enumerate()
        .map(|(pidx, pv)| {
            build_map.get(&pv.cloned())
        })
        .collect::<Vec<Option<&Vec<u32>>>>();
}

pub fn join_arrays(arrays1: &Vec<Box<dyn Array>>, arrays2: &Vec<Box<dyn Array>>) {
    // Selection of small vs large table
    let build = if arrays1[0].len() > arrays2[0].len() {arrays2} else {arrays1};
    let probe = if arrays1[0].len() > arrays2[0].len() {arrays1} else {arrays2};

    if arrays1.len() == 1 {
        // Build phase on smaller table
        let start = SystemTime::now();
        let build_map = array_to_idxs(build[0].as_ref()).unwrap();
        println!("Join Build build phase: {} ms", start.elapsed().unwrap().as_millis());

        // Probe phase on bigger table
        let start = SystemTime::now();
        let probe_idx = array_downcast_primitive(probe[0].as_ref());
        join_probe(probe_idx, &build_map);

        println!("Join Probe phase: {} ms", start.elapsed().unwrap().as_millis());        
    } else {
        // Build phase on smaller table
        let start = SystemTime::now();
        let build_idx = arrays_to_hash(build);
        let build_map = array_to_idxs(&build_idx);
        println!("Join Build build phase: {} ms", start.elapsed().unwrap().as_millis());

        // Probe phase on bigger table
        let start = SystemTime::now(); 
        let probe_idx = arrays_to_hash(probe);
        let probe_map = array_to_idxs(&probe_idx);

    };
} 