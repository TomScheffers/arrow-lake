use std::collections::HashMap;
use std::hash::Hash;
use std::cmp::{min, Eq};
use std::thread;

use rayon::prelude::*;

use arrow2::{
    types::NativeType,
    array::PrimitiveArray,
};

pub fn hashmap_to_str<K>(map: &HashMap<Option<K>, Vec<u32>>) -> HashMap<String, Vec<u32>> where K: std::fmt::Display {
    let mut map2 = HashMap::new();
    for (key, value) in map {
        match key {
            Some(key) => map2.insert(key.to_string(), value.clone()),
            None => map2.insert("".to_string(), value.clone())
        };
    }
    map2
}

pub fn hashmap_primitive_to_idxs<V: NativeType + Eq + Hash>(array: &PrimitiveArray<V>) -> HashMap<String, Vec<u32>> {
    let mut map = HashMap::new();
    for (i, a) in array.iter().enumerate() {
        let vec = map.entry(a).or_insert(Vec::new());
        vec.push(i as u32);                    
    }
    hashmap_to_str(&map)
}

pub fn hashmap_primitive_to_idxs_par<V: NativeType + Eq + Hash>(array: &PrimitiveArray<V>) -> HashMap<String, Vec<u32>> {
    let num_cpu: usize = thread::available_parallelism().unwrap().get();
    if array.len() > 5_000 {
        let size = array.len() / num_cpu;
        let maps = (0..num_cpu)
            .into_par_iter()
            .map(|i| {
                hashmap_primitive_to_idxs(&array.slice(i * size, min(size, array.len() - i * size)))
            })
            .collect::<Vec<HashMap<String, Vec<u32>>>>();
        hashmaps_merge_vec(maps)
    } else {
        hashmap_primitive_to_idxs(array)
    }
}

// Vectors of hashmaps
pub fn hashmaps_merge<K: Hash + Eq, V>(maps: Vec<HashMap<K, V>>) -> HashMap<K, Vec<V>> {
    let mut map = HashMap::new();
    for mapc in maps.into_iter() {
        for (k, v2) in mapc.into_iter() {
            let vec = map.entry(k).or_insert(Vec::new());
            vec.push(v2); // .and_modify(|v1: &mut Vec<V>| v1.push(v2));
        }
    }
    map
}

pub fn hashmaps_merge_vec<V>(maps: Vec<HashMap<String, Vec<V>>>) -> HashMap<String, Vec<V>> {
    let mut map = HashMap::new();
    for mapc in maps.into_iter() {
        for (k, mut v2) in mapc.into_iter() {
            let vec = map.entry(k).or_insert(Vec::new());
            vec.append(&mut v2);
            //for v2v in v2 {vec.push(v2v);}
        }
    }
    map
}