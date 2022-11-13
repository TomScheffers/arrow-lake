use std::collections::HashMap;
use std::cmp::Eq;

use rayon::prelude::*;

use arrow2::{
    types::NativeType,
    datatypes::*,
    array::{Array, PrimitiveArray},
    compute::take::take,
    compute::cast::{primitive_to_primitive},
    compute::arithmetics::basic::{add, add_scalar, mul_scalar},
    compute::aggregate::{min_primitive, max_primitive},
};

use crate::core::hm::hashmap_primitive_to_idxs_par;

// fn array_to_dictionary<V: NativeType + Hash + Eq>(array: &dyn Array) -> Result<DictionaryArray<i32>, Error> {
//     let arr = array.as_any().downcast_ref::<PrimitiveArray<V>>().unwrap();
//     let dict =     primitive_to_dictionary::<V, i32>(arr);
//     println!("{:?}", &(dict.unwrap()).keys());
//     primitive_to_dictionary::<V, i32>(arr)
// }

fn array_to_idxs(array: &dyn Array) -> Result<HashMap<String, Vec<u32>>, String> {
    match array.data_type() {
        DataType::Int8   => Ok(hashmap_primitive_to_idxs_par::<i8 >(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::Int16  => Ok(hashmap_primitive_to_idxs_par::<i16>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::Int32  => Ok(hashmap_primitive_to_idxs_par::<i32>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::Int64  => Ok(hashmap_primitive_to_idxs_par::<i64>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),         
        DataType::UInt8  => Ok(hashmap_primitive_to_idxs_par::<u8 >(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::UInt16 => Ok(hashmap_primitive_to_idxs_par::<u16>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::UInt32 => Ok(hashmap_primitive_to_idxs_par::<u32>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),        
        DataType::UInt64 => Ok(hashmap_primitive_to_idxs_par::<u64>(array.as_any().downcast_ref().expect("Downcast to primitive failed"))),         
        _ => Err(format!("{:?} is not implemented for hashing", array.data_type()))
    }    
}

pub fn array_take_idxs(array: &dyn Array, idxs: &Vec<u32>) -> Box<dyn Array> {
    let idxs = PrimitiveArray::from(idxs.iter().map(|x| Some(*x)).collect::<Vec<Option<u32>>>());
    take(array.as_ref(), &idxs).unwrap()
}

pub fn hash_group_merge(array: &dyn Array, map: &HashMap<Vec<String>, Vec<u32>>) -> HashMap<Vec<String>, Vec<u32>> {
    let mapr_iter = map 
        .par_iter()
        .map(|(k1, v1)| {
            let mapi = array_to_idxs(array_take_idxs(array, v1).as_ref()).unwrap();
            mapi
                .into_par_iter()
                .map(|(k2, v2)| {
                    let mut k3 = k1.clone();
                    k3.push(k2);
                    let v3 = v2.iter().map(|i| v1[(*i as usize)]).collect();
                    (k3, v3)
                })
                .collect::<Vec<(Vec<String>, Vec<u32>)>>()
        })
        .collect::<Vec<Vec<(Vec<String>, Vec<u32>)>>>();
    
    let mut mapr = HashMap::new();
    for mapi in mapr_iter {
        for (k, v) in mapi {
            mapr.insert(k, v);
        }
    }
    mapr
}

pub fn hash_group_merge_recur(arrays: Vec<&dyn Array>, map: &HashMap<Vec<String>, Vec<u32>>) -> HashMap<Vec<String>, Vec<u32>> {
    if arrays.len() > 0 {
        let map_new = hash_group_merge(arrays[0], map);
        hash_group_merge_recur(arrays[1..].to_vec(), &map_new)
    } else {
        map.clone()
    }
}

// Currently implementation: 1. groupby first array, 2. for each group in 1, take idxs and groupby array 2, repeat recursively
pub fn groupby_many(arrays: Vec<&dyn Array>) -> HashMap<Vec<String>, Vec<u32>> {
    let map1 = array_to_idxs(arrays[0]).unwrap().into_iter().map(|(k, v)| (vec![k], v)).collect::<HashMap<Vec<String>, Vec<u32>>>();
    if arrays.len() > 1 {
        hash_group_merge_recur(arrays[1..].to_vec(), &map1)
    } else {
        map1
    }
}

// Alternative implementation: 1. group all arrays parallel, 2. merge overlapping indexes for each combination of keys
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

fn arrays_to_hash(arrays: Vec<&dyn Array>) -> PrimitiveArray<u64> {
    let int_arrays = arrays.into_iter().map(|array| array_to_u64(array)).collect::<Vec<PrimitiveArray<u64>>>();
    let mut uidxs = int_arrays[0].clone();
    let mut max_last = max_primitive(&uidxs).unwrap();
    for iarr in int_arrays[1..].into_iter() {
        uidxs = mul_scalar(&uidxs, &max_last); // val2 = hash1 * max1 + hash2
        uidxs = add(&uidxs, iarr);
        max_last = max_primitive(&uidxs).unwrap();
    }
    uidxs
}

pub fn groupby_many_test(arrays: Vec<&dyn Array>) -> HashMap<Vec<String>, Vec<u32>> {
    // if arrays.len() == 1 {
    //     array_to_idxs(arrays[0]).unwrap().into_iter().map(|(k, v)| (vec![k], v)).collect::<HashMap<Vec<String>, Vec<u32>>>()
    // } else {
    let idxs = arrays_to_hash(arrays);
    let map = array_to_idxs(&idxs).unwrap().into_iter().map(|(k, v)| (vec![k], v)).collect::<HashMap<Vec<String>, Vec<u32>>>();
    map
}
