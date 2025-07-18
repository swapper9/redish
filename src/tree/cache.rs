use crate::config::{
    DEFAULT_INDEX_CACHE_LRU_MAX_CAPACITY, DEFAULT_INDEX_CACHE_MEMORY_LIMIT,
    DEFAULT_VALUE_CACHE_LRU_MAX_CAPACITY, DEFAULT_VALUE_CACHE_MEMORY_LIMIT,
};
use crate::tree::DataValue;
use std::collections::BTreeMap;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::path::PathBuf;

/// An LRU (Least Recently Used) cache for storing data values.
///
/// This cache is designed to store key-value pairs with automatic eviction
/// of least recently used items when capacity or memory limits are reached.
/// It provides fast access to frequently used data while maintaining bounded
/// memory usage.
///
/// # Cache Behavior
///
/// The cache operates with two primary constraints:
/// - **Capacity limit**: Maximum number of entries
/// - **Memory limit**: Maximum estimated memory usage
///
/// When either limit is exceeded, the least recently used entries are evicted
/// until the cache is within bounds.
///
/// # Thread Safety
///
/// This cache is **not** thread-safe. External synchronization is required
/// for concurrent access from multiple threads.
///
/// # Memory Management
///
/// Memory usage is estimated based on:
/// - Key size (path + key data)
/// - Value size (DataValue + actual data)
/// - Internal data structure overhead
///
/// # Performance Characteristics
///
/// - **Get operations**: O(1) average case
/// - **Put operations**: O(1) average case, O(n) worst case during eviction
/// - **Memory overhead**: Approximately 40-60 bytes per entry
///
/// # Examples
///
/// ```rust
/// // Create with default settings
/// let mut cache = LRUValueCache::default();
///
/// // Create with custom settings
/// let mut cache = LRUValueCacheBuilder::new()
///     .max_capacity(2000)
///     .memory_limit(64 * 1024 * 1024) // 64MB
///     .build();
///
/// // Store and retrieve values
/// let key = (PathBuf::from("table1"), b"key1".to_vec());
/// let value = DataValue::new(b"value1".to_vec(), None);
/// cache.put(key.clone(), value);
///
/// if let Some(cached_value) = cache.get(&key) {
///     println!("Cache hit!");
/// }
///
/// // Check statistics
/// let stats = cache.stats();
/// println!("Hit rate: {:.2}%", stats.hit_rate_percentage());
/// ```
/// # See Also
///
/// - [`LRUValueCacheBuilder`] - For building configured instances
/// - [`LRUIndexCache`] - For caching SSTable indexes
/// - [`CacheStats`] - For monitoring cache performance
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

    pub(crate) fn get(&mut self, sstable_path: &PathBuf, key: &[u8]) -> Option<DataValue> {
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

    pub(crate) fn put(&mut self, sstable_path: PathBuf, key: Vec<u8>, value: DataValue) {
        let cache_key = CacheKey { sstable_path, key };

        let value_size = self.estimate_value_size(&value);

        if let Some(old_value) = self.cache.get(&cache_key) {
            let old_size = self.estimate_value_size(old_value);
            self.current_memory_usage = self
                .current_memory_usage
                .saturating_sub(old_size)
                .saturating_add(value_size);
            self.cache.insert(cache_key.clone(), value);
            self.move_to_back(&cache_key);
            return;
        }

        while (self.cache.len() >= self.max_capacity
            || self.current_memory_usage + value_size > self.memory_limit)
            && !self.cache.is_empty()
        {
            if !self.evict_lru() {
                break;
            }
        }

        if self.cache.len() < self.max_capacity
            && self.current_memory_usage + value_size <= self.memory_limit
        {
            self.cache.insert(cache_key.clone(), value);
            self.lru_queue.push_back(cache_key);
            self.current_memory_usage += value_size;
        }
    }

    pub(crate) fn remove(&mut self, sstable_path: &PathBuf, key: &[u8]) {
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

    pub(crate) fn invalidate_sstable(&mut self, sstable_path: &PathBuf) {
        let keys_to_remove: Vec<CacheKey> = self
            .cache
            .keys()
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

/// Builder for configuring `LRUValueCache` instances.
///
/// This builder provides a fluent interface for setting up value cache parameters
/// including capacity limits, memory constraints, and performance tuning options.
pub struct LRUValueCacheBuilder {
    max_capacity: Option<usize>,
    memory_limit: Option<usize>,
}

impl LRUValueCacheBuilder {
    /// Creates a new builder with default values.
    ///
    /// All configuration options are initially unset and will use their
    /// default values when `build()` is called.
    ///
    /// # Returns
    /// A new `LRUValueCacheBuilder` instance
    pub fn new() -> Self {
        Self {
            max_capacity: None,
            memory_limit: None,
        }
    }

    /// Sets the maximum number of entries the cache can hold.
    ///
    /// When the cache reaches this limit, the least recently used entries
    /// will be evicted to make room for new ones. This acts as a hard limit
    /// on the number of cached values regardless of memory usage.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of cache entries
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Default
    /// Uses `DEFAULT_VALUE_CACHE_LRU_MAX_CAPACITY` if not specified
    pub fn max_capacity(mut self, capacity: usize) -> Self {
        self.max_capacity = Some(capacity);
        self
    }

    /// Sets the maximum memory the cache can use in bytes.
    ///
    /// The cache will evict entries when the estimated memory usage exceeds
    /// this limit, even if the entry count is below `max_capacity`. This
    /// provides memory-aware caching behavior.
    ///
    /// # Arguments
    /// * `limit` - Maximum memory usage in bytes
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Memory Estimation
    /// Memory usage is estimated based on:
    /// - Key size (path + key bytes)
    /// - Value size (data + metadata)
    /// - Internal data structure overhead
    ///
    /// # Default
    /// Uses `DEFAULT_VALUE_CACHE_MEMORY_LIMIT` if not specified
    pub fn memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    /// Builds the `LRUValueCache` with the configured settings.
    ///
    /// Any unspecified settings will use their default values from the
    /// configuration constants.
    ///
    /// # Returns
    /// A new `LRUValueCache` instance
    ///
    /// # Examples
    /// ```rust
    /// let cache = LRUValueCacheBuilder::new()
    ///     .max_capacity(1000)
    ///     .memory_limit(32 * 1024 * 1024)
    ///     .build();
    /// ```
    pub fn build(self) -> LRUValueCache {
        LRUValueCache::new(
            self.max_capacity
                .unwrap_or(DEFAULT_VALUE_CACHE_LRU_MAX_CAPACITY),
            self.memory_limit
                .unwrap_or(DEFAULT_VALUE_CACHE_MEMORY_LIMIT),
        )
    }
}

impl Default for LRUValueCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// An LRU (Least Recently Used) cache for storing SSTable indexes.
///
/// This cache stores the complete index structure of SSTable files in memory
/// to avoid repeated disk I/O operations during key lookups. Each cached entry
/// represents the full index of one SSTable file, containing key-to-offset
/// mappings for efficient random access.
///
/// # Cache Behavior
///
/// The cache operates with two primary constraints:
/// - **Capacity limit**: Maximum number of SSTable indexes
/// - **Memory limit**: Maximum estimated memory usage
///
/// When either limit is exceeded, the least recently used indexes are evicted
/// until the cache is within bounds.
///
/// # Index Structure
///
/// Each cached index contains:
/// - **Key mappings**: BTreeMap of keys to file offsets
/// - **Metadata**: File path and size information
/// - **Access tracking**: LRU position and statistics
///
/// # Thread Safety
///
/// This cache is **not** thread-safe. External synchronization is required
/// for concurrent access from multiple threads.
///
/// # Memory Management
///
/// Memory usage is estimated based on:
/// - Key data size (actual key bytes)
/// - Offset data (8 bytes per key)
/// - BTreeMap overhead (approximately 24 bytes per node)
/// - Path storage (file path strings)
///
/// # Performance Characteristics
///
/// - **Get operations**: O(1) for cache lookup + O(log n) for key search
/// - **Put operations**: O(1) average case, O(m) worst case during eviction
/// - **Memory overhead**: Approximately 32-48 bytes per cached key
///
/// # Examples
///
/// ```rust
/// use redish::tree::{LRUIndexCache, LRUIndexCacheBuilder};
/// use std::path::PathBuf;
/// use std::collections::BTreeMap;
///
/// // Create with default settings
/// let mut cache = LRUIndexCache::default();
///
/// // Create with custom settings
/// let mut cache = LRUIndexCacheBuilder::new()
///     .max_capacity(300)
///     .memory_limit(32 * 1024 * 1024) // 32MB
///     .build();
///
/// // Cache an SSTable index
/// let table_path = PathBuf::from("sstable_001.sst");
/// let mut index = BTreeMap::new();
/// index.insert(b"key1".to_vec(), 0u64);
/// index.insert(b"key2".to_vec(), 1024u64);
///
/// cache.put(table_path.clone(), index);
///
/// // Retrieve cached index
/// if let Some(cached_index) = cache.get(&table_path) {
///     if let Some(offset) = cached_index.get(b"key1") {
///         println!("Found key1 at offset {}", offset);
///     }
/// }
///
/// // Check statistics
/// let stats = cache.stats();
/// println!("Cache efficiency: {:.2}%", stats.hit_rate_percentage());
/// ```
/// # See Also
///
/// - [`LRUIndexCacheBuilder`] - For building configured instances
/// - [`LRUValueCache`] - For caching data values
/// - [`CacheStats`] - For monitoring cache performance
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

    pub(crate) fn get(&mut self, path: &PathBuf) -> Option<&BTreeMap<Vec<u8>, u64>> {
        if self.cache.contains_key(path) {
            self.hit_count += 1;
            self.move_to_back(path);
            self.cache.get(path)
        } else {
            self.miss_count += 1;
            None
        }
    }

    pub(crate) fn put(&mut self, path: PathBuf, index: BTreeMap<Vec<u8>, u64>) {
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

    pub(crate) fn remove(&mut self, path: &PathBuf) {
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

    /// Returns the number of cached SSTable indexes.
    ///
    /// This method provides the current count of SSTable indexes stored in the cache.
    /// Each cached entry represents one SSTable file's index data.
    ///
    /// # Returns
    /// The number of SSTable indexes currently cached
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Checks if the index cache is empty.
    ///
    /// Returns `true` if the cache contains no SSTable indexes, `false` otherwise.
    /// This is equivalent to checking if `len() == 0` but may be more semantically clear.
    ///
    /// # Returns
    /// `true` if the cache is empty, `false` if it contains at least one entry
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Returns a list of all cached SSTable file paths.
    ///
    /// This method provides visibility into which SSTable files currently have
    /// their indexes cached in memory. The returned paths are cloned from the
    /// internal cache keys.
    ///
    /// # Returns
    /// A `Vec<PathBuf>` containing the file paths of all cached SSTable indexes
    pub fn cached_paths(&self) -> Vec<PathBuf> {
        self.cache.keys().cloned().collect()
    }

    /// Resizes the cache with new capacity and memory limits.
    ///
    /// This method updates the cache's maximum capacity and memory limit settings.
    /// If the new limits are smaller than the current cache size, it will evict
    /// the least recently used entries until the cache fits within the new constraints.
    ///
    /// # Arguments
    /// * `new_capacity` - The new maximum number of entries the cache can hold
    /// * `new_memory_limit` - The new maximum memory usage in bytes
    ///
    /// # Behavior
    /// - Updates internal capacity and memory limit settings
    /// - Evicts LRU entries if current size exceeds new limits
    /// - Maintains LRU ordering during eviction
    /// - Stops eviction if cache becomes empty
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

/// Builder for configuring `LRUIndexCache` instances.
///
/// This builder provides a fluent interface for setting up index cache parameters
/// including capacity limits, memory constraints, and performance tuning options.
pub struct LRUIndexCacheBuilder {
    max_capacity: Option<usize>,
    memory_limit: Option<usize>,
}

impl LRUIndexCacheBuilder {
    /// Creates a new builder with default values.
    ///
    /// All configuration options are initially unset and will use their
    /// default values when `build()` is called.
    ///
    /// # Returns
    /// A new `LRUIndexCacheBuilder` instance
    ///
    /// # Examples
    /// ```rust
    /// let builder = LRUIndexCacheBuilder::new();
    /// let cache = builder.build(); // Uses all defaults
    /// ```
    pub fn new() -> Self {
        Self {
            max_capacity: None,
            memory_limit: None,
        }
    }

    /// Sets the maximum number of SSTable indexes the cache can hold.
    ///
    /// Each entry in the index cache corresponds to one SSTable file's
    /// complete index. When the cache reaches this limit, the least recently
    /// used indexes will be evicted to make room for new ones.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of cached SSTable indexes
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Examples
    /// ```rust
    /// let cache = LRUIndexCacheBuilder::new()
    ///     .max_capacity(300)
    ///     .build();
    /// ```
    ///
    /// # Performance Impact
    /// - **Higher values**: Fewer disk reads for index lookups
    /// - **Lower values**: More frequent index reloading from disk
    ///
    /// # Sizing Guidelines
    /// - Set to roughly match your expected number of active SSTable files
    /// - Consider read patterns and working set size
    /// - Balance with available memory constraints
    ///
    /// # Default
    /// Uses `DEFAULT_INDEX_CACHE_LRU_MAX_CAPACITY` if not specified
    pub fn max_capacity(mut self, capacity: usize) -> Self {
        self.max_capacity = Some(capacity);
        self
    }

    /// Sets the maximum memory the cache can use in bytes.
    ///
    /// The cache will evict indexes when the estimated memory usage exceeds
    /// this limit. Index memory usage depends on the number of keys in each
    /// SSTable and the size of those keys.
    ///
    /// # Arguments
    /// * `limit` - Maximum memory usage in bytes
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Examples
    /// ```rust
    /// let cache = LRUIndexCacheBuilder::new()
    ///     .memory_limit(32 * 1024 * 1024) // 32MB limit
    ///     .build();
    /// ```
    ///
    /// # Memory Estimation
    /// Memory usage includes:
    /// - Key data (actual key bytes)
    /// - File offset information (8 bytes per key)
    /// - BTreeMap overhead for index structure
    /// - Path information for each cached SSTable
    ///
    /// # Performance Impact
    /// - **Higher limits**: Better index cache hit rates
    /// - **Lower limits**: More frequent index reloading
    ///
    /// # Default
    /// Uses `DEFAULT_INDEX_CACHE_MEMORY_LIMIT` if not specified
    pub fn memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    /// Builds the `LRUIndexCache` with the configured settings.
    ///
    /// Any unspecified settings will use their default values from the
    /// configuration constants.
    ///
    /// # Returns
    /// A new `LRUIndexCache` instance
    ///
    /// # Examples
    /// ```rust
    /// let cache = LRUIndexCacheBuilder::new()
    ///     .max_capacity(150)
    ///     .memory_limit(16 * 1024 * 1024)
    ///     .build();
    /// ```
    pub fn build(self) -> LRUIndexCache {
        LRUIndexCache::new(
            self.max_capacity
                .unwrap_or(DEFAULT_INDEX_CACHE_LRU_MAX_CAPACITY),
            self.memory_limit
                .unwrap_or(DEFAULT_INDEX_CACHE_MEMORY_LIMIT),
        )
    }
}

impl Default for LRUIndexCacheBuilder {
    fn default() -> Self {
        Self::new()
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

impl fmt::Display for CacheStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let memory_limit_mb = self.memory_limit as f64 / (1024.0 * 1024.0);
        let memory_utilization_bytes =
            (self.memory_utilization * self.memory_limit as f64) as usize;
        let memory_utilization_mb = memory_utilization_bytes as f64 / (1024.0 * 1024.0);
        let memory_utilization_percent = self.memory_utilization * 100.0;

        let (limit_value, limit_unit) = if memory_limit_mb >= 1.0 {
            (memory_limit_mb, "MB")
        } else {
            (self.memory_limit as f64 / 1024.0, "KB")
        };

        let (utilization_value, utilization_unit) = if memory_utilization_mb >= 1.0 {
            (memory_utilization_mb, "MB")
        } else {
            (memory_utilization_bytes as f64 / 1024.0, "KB")
        };

        write!(
            f,
            "Cache Stats: {} entries, {} hits, {} misses, {} evictions, {:.1}% hit rate, Memory: {:.2} {} / {:.2} {} ({:.1}%)",
            self.size,
            self.hit_count,
            self.miss_count,
            self.eviction_count,
            self.hit_rate * 100.0,
            utilization_value,
            utilization_unit,
            limit_value,
            limit_unit,
            memory_utilization_percent
        )
    }
}
