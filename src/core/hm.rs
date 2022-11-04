use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::cmp::{min, Eq};
use std::thread;

use rayon::prelude::*;

use arrow2::{
    types::NativeType,
    array::{Array, PrimitiveArray},
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

// Merges hashmaps into one
// m1 = {m11: [1, 2, 3], m12: [4, 5, 6]}, m2 = {m21: [1, 3, 5], m22: [2, 4, 6]} -> mr = {mr1: [1, 3], mr2: [2], mr3: [5], mr4: [4, 6]}
fn intersect<V>(vecs: Vec<Vec<V>>) -> Vec<V>  where V: Eq + Clone + Copy + Hash {
    let mut result: Vec<V> = vecs[0].clone();

    for vec in vecs {
        let uniq: HashSet<V> = vec.into_iter().collect();
        result = uniq
            .intersection(&result.into_iter().collect())
            .map(|i| *i)
            .collect::<Vec<V>>();
    }
    result
}

// fn combinations<V>(Vec<HashMap<String, Vec<V>>>>) -> HashMap<Vec<String>, Vec<Vec<V>>> {}

// pub fn hashmaps_merge_intersect<V>(columns: Vec<String>, maps: Vec<HashMap<String, Vec<V>>>) -> HashMap<HashMap<String, String>, Vec<V>> {
//     let mut map = HashMap::new();
//     let combinations = columns
//         .iter()
//         .zip(maps)
//         .map(|(c, m)| {
//         })


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersect() {
        let v1 = vec![1, 2, 3, 4];
        let v2 = vec![2, 4];
        let v3 = intersect(vec![v1, v2]);
        assert_eq!(v3, vec![2, 4])
    }
}
