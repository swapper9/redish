use std::collections::{HashMap, VecDeque};
use std::collections::BTreeMap;
use std::path::PathBuf;
use crate::config::{DEFAULT_INDEX_CACHE_LRU_MAX_CAPACITY, DEFAULT_INDEX_CACHE_MEMORY_LIMIT, DEFAULT_VALUE_CACHE_LRU_MAX_CAPACITY, DEFAULT_VALUE_CACHE_MEMORY_LIMIT};
use crate::tree::DataValue;

pub struct LRUValueCache {
    cache: HashMap<CacheKey, DataValue>,
    lru_queue: VecDeque<CacheKey>,
    max_capacity: usize,
    memory_limit: usize,
    current_memory_usage: usize,
    hit_count: u64,
    miss_count: u64,
    eviction_count: u64,
}

impl Default for LRUValueCache {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            max_capacity: DEFAULT_VALUE_CACHE_LRU_MAX_CAPACITY,
            memory_limit: DEFAULT_VALUE_CACHE_MEMORY_LIMIT,
            current_memory_usage: 0,
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
        }
    }
}

impl LRUValueCache {
    pub fn new(max_capacity: usize, memory_limit: usize) -> Self {
        Self {
            cache: HashMap::with_capacity(max_capacity),
            lru_queue: VecDeque::with_capacity(max_capacity),
            max_capacity,
            memory_limit,
            current_memory_usage: 0,
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
        }
    }

    pub fn get(&mut self, sstable_path: &PathBuf, key: &[u8]) -> Option<DataValue> {
        let cache_key = CacheKey {
            sstable_path: sstable_path.clone(),
            key: key.to_vec(),
        };

        if let Some(value) = self.cache.get(&cache_key).cloned() {
            self.hit_count += 1;
            self.move_to_back(&cache_key);
            Some(value)
        } else {
            self.miss_count += 1;
            None
        }
    }

    pub fn put(&mut self, sstable_path: PathBuf, key: Vec<u8>, value: DataValue) {
        let cache_key = CacheKey {
            sstable_path,
            key,
        };

        let value_size = self.estimate_value_size(&value);

        if let Some(old_value) = self.cache.get(&cache_key) {
            let old_size = self.estimate_value_size(old_value);
            self.current_memory_usage = self.current_memory_usage
                .saturating_sub(old_size)
                .saturating_add(value_size);
            self.cache.insert(cache_key.clone(), value);
            self.move_to_back(&cache_key);
            return;
        }

        while (self.cache.len() >= self.max_capacity ||
            self.current_memory_usage + value_size > self.memory_limit) &&
            !self.cache.is_empty() {
            if !self.evict_lru() {
                break;
            }
        }

        if self.cache.len() < self.max_capacity &&
            self.current_memory_usage + value_size <= self.memory_limit {
            self.cache.insert(cache_key.clone(), value);
            self.lru_queue.push_back(cache_key);
            self.current_memory_usage += value_size;
        }
    }

    pub fn remove(&mut self, sstable_path: &PathBuf, key: &[u8]) {
        let cache_key = CacheKey {
            sstable_path: sstable_path.clone(),
            key: key.to_vec(),
        };

        if let Some(value) = self.cache.remove(&cache_key) {
            let value_size = self.estimate_value_size(&value);
            self.current_memory_usage = self.current_memory_usage.saturating_sub(value_size);
            self.lru_queue.retain(|k| k != &cache_key);
        }
    }

    pub fn invalidate_sstable(&mut self, sstable_path: &PathBuf) {
        let keys_to_remove: Vec<CacheKey> = self.cache.keys()
            .filter(|k| &k.sstable_path == sstable_path)
            .cloned()
            .collect();

        for key in keys_to_remove {
            self.remove(&key.sstable_path, &key.key);
        }
    }

    fn move_to_back(&mut self, cache_key: &CacheKey) {
        if let Some(pos) = self.lru_queue.iter().position(|k| k == cache_key) {
            let key = self.lru_queue.remove(pos).unwrap();
            self.lru_queue.push_back(key);
        }
    }

    fn evict_lru(&mut self) -> bool {
        if let Some(lru_key) = self.lru_queue.pop_front() {
            if let Some(value) = self.cache.remove(&lru_key) {
                let value_size = self.estimate_value_size(&value);
                self.current_memory_usage = self.current_memory_usage.saturating_sub(value_size);
                self.eviction_count += 1;
                return true;
            }
        }
        false
    }

    fn estimate_value_size(&self, value: &DataValue) -> usize {
        size_of::<DataValue>() + value.get_data().len()
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            eviction_count: self.eviction_count,
            hit_rate: if self.hit_count + self.miss_count > 0 {
                self.hit_count as f64 / (self.hit_count + self.miss_count) as f64
            } else {
                0.0
            },
            memory_limit: self.memory_limit,
            memory_utilization: if self.memory_limit > 0 {
                self.current_memory_usage as f64 / self.memory_limit as f64
            } else {
                0.0
            },
        }
    }

    pub fn clear(&mut self) {
        self.cache.clear();
        self.lru_queue.clear();
        self.current_memory_usage = 0;
        self.hit_count = 0;
        self.miss_count = 0;
        self.eviction_count = 0;
    }
}

pub struct LRUIndexCache {
    cache: HashMap<PathBuf, BTreeMap<Vec<u8>, u64>>,
    pub lru_queue: VecDeque<PathBuf>,
    max_capacity: usize,
    memory_limit: usize,
    current_memory_usage: usize,
    hit_count: u64,
    miss_count: u64,
    eviction_count: u64,
}

impl Default for LRUIndexCache {
    fn default() -> Self {
        Self {
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            max_capacity: DEFAULT_INDEX_CACHE_LRU_MAX_CAPACITY,
            memory_limit: DEFAULT_INDEX_CACHE_MEMORY_LIMIT,
            current_memory_usage: 0,
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
        }
    }
}

impl LRUIndexCache {
    pub fn new(max_capacity: usize, memory_limit: usize) -> Self {
        Self {
            cache: HashMap::new(),
            lru_queue: VecDeque::new(),
            max_capacity,
            memory_limit,
            current_memory_usage: 0,
            hit_count: 0,
            miss_count: 0,
            eviction_count: 0,
        }
    }

    pub fn get(&mut self, path: &PathBuf) -> Option<&BTreeMap<Vec<u8>, u64>> {
        if self.cache.contains_key(path) {
            self.hit_count += 1;
            self.move_to_back(path);
            self.cache.get(path)
        } else {
            self.miss_count += 1;
            None
        }
    }

    pub fn put(&mut self, path: PathBuf, index: BTreeMap<Vec<u8>, u64>) {
        let index_size = self.estimate_index_size(&index);

        if self.cache.contains_key(&path) {
            let old_size = self.estimate_index_size(self.cache.get(&path).unwrap());
            self.current_memory_usage = self.current_memory_usage.saturating_sub(old_size);
            self.cache.insert(path.clone(), index);
            self.current_memory_usage += index_size;
            self.move_to_back(&path);
            return;
        }

        while (self.cache.len() >= self.max_capacity)
            || (self.current_memory_usage + index_size > self.memory_limit)
        {
            if !self.evict_lru() {
                break;
            }
        }

        self.cache.insert(path.clone(), index);
        self.lru_queue.push_back(path);
        self.current_memory_usage += index_size;
    }

    pub fn remove(&mut self, path: &PathBuf) {
        if self.cache.contains_key(path) {
            self.cache.remove(path);
        }
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            hit_count: self.hit_count,
            miss_count: self.miss_count,
            eviction_count: self.eviction_count,
            hit_rate: if self.hit_count + self.miss_count > 0 {
                self.hit_count as f64 / (self.hit_count + self.miss_count) as f64
            } else {
                0.0
            },
            memory_limit: self.memory_limit,
            memory_utilization: self.current_memory_usage as f64 / self.memory_limit as f64,
        }
    }

    fn move_to_back(&mut self, path: &PathBuf) {
        self.lru_queue.retain(|p| p != path);
        self.lru_queue.push_back(path.clone());
    }

    fn evict_lru(&mut self) -> bool {
        if let Some(path) = self.lru_queue.pop_front() {
            if let Some(index) = self.cache.remove(&path) {
                let index_size = self.estimate_index_size(&index);
                self.current_memory_usage = self.current_memory_usage.saturating_sub(index_size);
                self.eviction_count += 1;
                return true;
            }
        }
        false
    }

    fn estimate_index_size(&self, index: &BTreeMap<Vec<u8>, u64>) -> usize {
        let mut size = 0;
        for (key, _) in index {
            size += key.len() + 8;
            size += key.capacity();
            size += size_of::<Vec<u8>>();
        }
        size += index.len() * 28;
        size += size_of::<BTreeMap<Vec<u8>, u64>>();
        size
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn cached_paths(&self) -> Vec<PathBuf> {
        self.cache.keys().cloned().collect()
    }

    pub fn evict_n(&mut self, n: usize) -> usize {
        let mut evicted = 0;
        for _ in 0..n {
            if self.evict_lru() {
                evicted += 1;
            } else {
                break;
            }
        }
        evicted
    }

    pub fn resize(&mut self, new_capacity: usize, new_memory_limit: usize) {
        self.max_capacity = new_capacity;
        self.memory_limit = new_memory_limit;

        while (self.cache.len() > self.max_capacity)
            || (self.current_memory_usage > self.memory_limit)
        {
            if !self.evict_lru() {
                break;
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.cache.clear();
        self.lru_queue.clear();
        self.current_memory_usage = 0;
    }

    pub fn contains_key(&mut self, key: &PathBuf) -> bool {
        self.get(key).is_some()
    }
}

#[derive(Clone, Debug)]
pub struct CacheKey {
    pub sstable_path: PathBuf,
    pub key: Vec<u8>,
}

impl std::hash::Hash for CacheKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sstable_path.hash(state);
        self.key.hash(state);
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.sstable_path == other.sstable_path && self.key == other.key
    }
}

impl Eq for CacheKey {}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
    pub hit_rate: f64,
    pub memory_limit: usize,
    pub memory_utilization: f64,
}
