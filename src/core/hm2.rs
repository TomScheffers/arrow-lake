use std::time::SystemTime;

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::cmp::{min, Eq};

use rayon::prelude::*;

use arrow2::{
    types::NativeType,
    array::PrimitiveArray,
};

pub fn hashmap_to_kv<'a, K, V>(map: &'a HashMap<K, V>) -> (Vec<&'a K>, Vec<&'a V>) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for (key, value) in map {
        keys.push(key);
        values.push(value);
    }
    (keys, values)
}

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

pub fn hashmap_from_vecs<V>(keys: Vec<&String>, values: Vec<V>) -> HashMap<String, V> {
    let mut map = HashMap::new();
    for (key, value) in keys.iter().zip(values) {
        map.insert((*key).clone(), value);
    }
    map
}

// PrimitiveArray to Multiple indexes (for groupby & join)

pub fn hashmap_primitive_to_idxs<V: NativeType + Eq + Hash>(array: &PrimitiveArray<V>) -> HashMap<Option<V>, Vec<u32>> {
    let mut map = HashMap::new();
    for (i, a) in array.iter().enumerate() {
        let vec = map.entry(a.cloned()).or_insert(Vec::new());
        vec.push(i as u32);                    
    }
    map
}

// THIS IS SLOW FOR LARGE HASHMAPS BECAUSE WE CANNOT 
pub fn hashmaps_merge_idxs<V: NativeType + Eq + Hash>(maps: Vec<HashMap<Option<V>, Vec<u32>>>) -> HashMap<Option<V>, Vec<u32>> {
    let mut map = HashMap::new();
    for mapc in maps.into_iter() {
        for (k, mut v2) in mapc.into_iter() {
            let vec = map.entry(k).or_insert(Vec::new());
            vec.append(&mut v2);
        }
    }
    map
}

// WE COULD PARALLELIZE BY MERGING TUPLES [h1, h2, h3, h4, h5] -> [[h1, h2], [[h3, h4], h5]]

// THIS IS VERY SLOW (FIRST FIND KEYS THEN PARALLEL GET)
pub fn hashmaps_merge_vec_test<V: NativeType + Eq + Hash>(maps: Vec<HashMap<Option<V>, Vec<u32>>>) -> HashMap<Option<V>, Vec<u32>> {
    let mut keys = HashSet::new();
    for map in &maps {
        let new_keys = &map.keys().collect::<HashSet<&Option<V>>>();
        keys = keys.union(new_keys).map(|x| *x).collect::<HashSet<&Option<V>>>();
    }
    let map = keys
        .into_par_iter()
        .map(|k| {
            let mut vec = Vec::new();
            for map in &maps {
                let vm = map.get(&k);
                if vm.is_some() {
                    let mut vmc = vm.unwrap().clone();
                    vec.append(&mut vmc)
                }
            }
            (*k, vec)
        })
        .collect::<HashMap<Option<V>, Vec<u32>>>();
    map
}

pub fn hashmap_primitive_to_idxs_par<V: NativeType + Eq + Hash>(array: &PrimitiveArray<V>) -> HashMap<Option<V>, Vec<u32>> {
    let size = 10_000;
    let workers: usize = 24; //thread::available_parallelism().unwrap().get();
    println!("Workers: {}", workers);
    if array.len() > size {
        let start = SystemTime::now();
        let size = array.len() / workers + 1;
        let maps = (0..workers)
            .into_par_iter()
            .map(|i| {
                hashmap_primitive_to_idxs(&array.slice(i * size, min(size, array.len() - i * size)))
            })
            .collect::<Vec<HashMap<Option<V>, Vec<u32>>>>();
        println!("HM PH1: {} ms", start.elapsed().unwrap().as_millis());

        let start = SystemTime::now();
        let res = hashmaps_merge_idxs(maps);
        println!("HM PH2: {} ms", start.elapsed().unwrap().as_millis());
        res
    } else {
        hashmap_primitive_to_idxs(array)
    }
}

// PrimitiveArray to Single index (for merge)
pub fn hashmaps_merge_idx<V: NativeType + Eq + Hash>(maps: Vec<HashMap<Option<V>, u32>>) -> HashMap<Option<V>, u32> {
    maps.into_iter().flat_map(|m| m.into_iter()).collect::<HashMap<Option<V>, u32>>()
}

pub fn hashmap_primitive_to_idx<V: NativeType + Eq + Hash>(array: &PrimitiveArray<V>) -> HashMap<Option<V>, u32> {
    array.into_iter().enumerate().map(|(i, a)| (a.cloned(), i as u32)).collect::<HashMap<Option<V>, u32>>()
}

pub fn hashmap_primitive_to_idx_par<V: NativeType + Eq + Hash>(array: &PrimitiveArray<V>) -> HashMap<Option<V>, u32> {
    let size = 10_000;
    let workers: usize = 24; //thread::available_parallelism().unwrap().get();
    if array.len() > size {
        let start = SystemTime::now();
        let size = array.len() / workers + 1;
        let maps = (0..workers)
            .into_par_iter()
            .map(|i| {
                hashmap_primitive_to_idx(&array.slice(i * size, min(size, array.len() - i * size)))
            })
            .collect::<Vec<HashMap<Option<V>, u32>>>();
        hashmaps_merge_idx(maps)
    } else {
        hashmap_primitive_to_idx(array)
    }
}
