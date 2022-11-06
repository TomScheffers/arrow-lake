use std::time::SystemTime;

use std::collections::{HashSet, HashMap};
use std::hash::Hash;
use std::cmp::{min, Eq};

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
    compute::contains::contains,
    error::Error,
};

use crate::core::hm2::{hashmap_primitive_to_idx_par};

fn array_to_idx(array: &dyn Array) -> Result<Vec<HashMap<Option<i64>, u32>>, String> {
    match array.data_type() {
        DataType::Int64  => Ok(hashmap_primitive_to_idx_par::<i64>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),         
        _ => Err(format!("{:?} is not implemented for hashing", array.data_type()))
    }    
}

fn array_downcast_primitive<T: NativeType + Eq>(array: &dyn Array) -> &PrimitiveArray<T> {
    array.as_any().downcast_ref::<PrimitiveArray<T>>().expect("Downcast to primitive failed")
}

fn array_to_u64(array: &dyn Array) -> PrimitiveArray<i64> {
    let arr = match array.data_type() {
        DataType::Int64  => Ok(array_downcast_primitive::<i64>(array)),               
        _ => Err(format!("{:?} is not implemented for hashing", array.data_type()))
    }; 
    let arr = arr.unwrap(); 
    let min_value = min_primitive(arr).unwrap();
    let parr = add_scalar(arr, &min_value);
    parr
    //primitive_to_primitive(&parr, &DataType::Int64)
}

fn arrays_to_hash(arrays: &Vec<Box<dyn Array>>) -> PrimitiveArray<i64> {
    let int_arrays = arrays.into_iter().map(|array| array_to_u64(array.as_ref())).collect::<Vec<PrimitiveArray<i64>>>();
    let mut uidxs = int_arrays[0].clone();
    let mut max_last = max_primitive(&uidxs).unwrap();
    for iarr in int_arrays[1..].into_iter() {
        uidxs = mul_scalar(&uidxs, &max_last); // val2 = hash1 * max1 + hash2
        uidxs = add(&uidxs, iarr);
        max_last = max_primitive(&uidxs).unwrap();
    }
    uidxs
}

// Return left idxs and right idxs which forms a unique table
fn arrays_to_array(arrays: &Vec<Box<dyn Array>>) -> &PrimitiveArray<i64> {
    array_downcast_primitive::<i64>(arrays[0].as_ref())

    // if arrays.len() == 1 {
    //     array_downcast_primitive::<i64>(arrays[0])
    // } else {
    //     arrays_to_hash(arrays)
    // }
}

pub fn arrays_to_idx(arrays: &Vec<Box<dyn Array>>) -> Result<Vec<HashMap<Option<i64>, u32>>, String> {
    let array = arrays_to_array(arrays);
    array_to_idx(array)
}

pub fn merge_arrays(left: &Vec<Box<dyn Array>>, right: &Vec<Box<dyn Array>>) -> (Vec<u32>, Vec<u32>) {
    // Build probe on right side
    let start = SystemTime::now();
    let right_maps = arrays_to_idx(right).unwrap();
    println!("Merging Right map phase: {} ms", start.elapsed().unwrap().as_millis());

    // Prepare probe array
    let left_array = arrays_to_array(left);

    // Loop over left side: keep boolean mask of left side, set to true when value not in right_map
    let start = SystemTime::now();
    let workers = 100;
    let size = left_array.len() / workers + 1;
    let left_idxs = (0..workers)
        .into_par_iter()
        .map(|i| {
            left_array.slice(i * size, min(size, left_array.len() - i * size))
                .iter()
                .enumerate()
                .filter(|(i, lv)| {
                    // Check if value is in any of the right_maps
                    for right_map in &right_maps {
                        if right_map.get(&lv.cloned()).is_some() {
                            return false
                        }
                    }
                    true
                })
                .map(|(i, lv)| i as u32)
                .collect::<Vec<u32>>()
        })
        .collect::<Vec<Vec<u32>>>();
    let left_idxs = left_idxs.into_iter().flat_map(|m| m.into_iter()).collect::<Vec<u32>>();
    println!("Merging Left mask phase: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let right_idxs = right_maps.into_iter().flat_map(|m| m.into_values()).collect::<Vec<u32>>();
    println!("Merging Right idx gather: {} ms", start.elapsed().unwrap().as_millis());

    (left_idxs, right_idxs)
} 