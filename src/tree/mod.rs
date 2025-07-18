pub mod cache;
pub mod data_value;
pub mod settings;
mod sstable;
mod test;
mod compression;

pub use cache::*;
pub use compression::*;
pub use data_value::*;
pub use settings::*;

use crate::config::DEFAULT_DB_PATH;
use crate::{logger, util};
use bincode::Encode;
use log::warn;
use once_cell::sync::Lazy;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::error::Error;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

static INIT: Lazy<()> = Lazy::new(|| {
    logger::init_logger().expect("Logger was not initialized!");
});

pub struct Tree {
    mem_table: BTreeMap<Vec<u8>, DataValue>,
    immutable_mem_tables: VecDeque<BTreeMap<Vec<u8>, DataValue>>,
    ss_tables: Vec<PathBuf>,
    settings: TreeSettings,
    index_cache: LRUIndexCache,
    value_cache: LRUValueCache,
    compression_stats: CompressionStats
}

impl Drop for Tree {
    fn drop(&mut self) {
        self.flush();
    }
}

impl Tree {
    /// Creates a new empty Tree with default settings.
    ///
    /// Initializes the logger and displays the application logo.
    ///
    /// # Returns
    /// A new Tree instance with default configuration
    pub fn new() -> Self {
        Lazy::force(&INIT);
        util::logo();

        Self {
            mem_table: BTreeMap::new(),
            immutable_mem_tables: VecDeque::new(),
            ss_tables: Vec::new(),
            settings: TreeSettings::default(),
            index_cache: LRUIndexCache::default(),
            value_cache: LRUValueCache::default(),
            compression_stats: CompressionStats::default(),
        }
    }

    /// Creates a new Tree with a specific database path.
    ///
    /// # Arguments
    /// * `path` - The database directory path
    ///
    /// # Returns
    /// A new Tree instance configured with the specified path
    pub fn new_with_path(path: &str) -> Self {
        let mut tree = Self::new();
        tree.settings = TreeSettings {
            db_path: PathBuf::from(path),
            ..TreeSettings::default()
        };
        tree
    }

    /// Creates a new Tree with custom settings.
    ///
    /// # Arguments
    /// * `settings` - TreeSettings configuration
    ///
    /// # Returns
    /// A new Tree instance with the specified settings
    pub fn new_with_settings(settings: TreeSettings) -> Self {
        let mut tree = Self::new();
        tree.settings = settings;
        tree
    }

    /// Retrieves statistics for the index cache.
    ///
    /// Returns detailed performance metrics about the index cache, including
    /// hit/miss ratios, memory usage, and eviction counts. This information
    /// can be used to monitor cache performance and optimize cache settings.
    ///
    /// # Returns
    /// A `CacheStats` struct containing:
    /// - Size: Current number of cached entries
    /// - Hit count: Number of successful cache lookups
    /// - Miss count: Number of cache misses
    /// - Eviction count: Number of entries evicted from cache
    /// - Hit rate: Percentage of successful cache hits
    /// - Memory utilization: Current memory usage percentage
    pub fn get_index_cache_stats(&self) -> CacheStats {
        self.index_cache.stats()
    }

    /// Retrieves statistics for the value cache.
    ///
    /// Returns detailed performance metrics about the value cache, including
    /// hit/miss ratios, memory usage, and eviction counts. This information
    /// helps monitor how effectively the value cache is improving read performance.
    ///
    /// # Returns
    /// A `CacheStats` struct containing cache performance metrics
    pub fn get_value_cache_stats(&self) -> CacheStats {
        self.value_cache.stats()
    }

    /// Retrieves compression statistics for the tree.
    ///
    /// Returns a reference to the compression statistics that track compression
    /// and decompression operations performed by the tree. This includes timing
    /// information, compression ratios, and operation counts.
    ///
    /// # Returns
    /// A reference to `CompressionStats` containing:
    /// - Total operations performed
    /// - Original and compressed data sizes
    /// - Compression and decompression timings
    /// - Compression ratio statistics
    pub fn get_compression_stats(&self) -> &CompressionStats {
        &self.compression_stats
    }

    /// Resets all compression statistics to their initial values.
    ///
    /// This method clears all accumulated compression statistics, including
    /// operation counts, timing data, and compression ratios. Useful for
    /// benchmarking specific operations or periodically resetting metrics.
    ///
    /// # Effects
    /// - Resets all counters to zero
    /// - Clears timing statistics
    /// - Resets compression ratio calculations
    /// - Does not affect actual compressed data
    pub fn reset_compression_stats(&mut self) {
        self.compression_stats.reset();
    }

    /// Clears all entries from the index cache.
    ///
    /// This method removes all cached SSTable indexes from memory, forcing
    /// subsequent reads to reload index data from disk. This can be useful
    /// for freeing memory or ensuring fresh index data is loaded.
    pub fn clear_index_cache(&mut self) {
        self.index_cache.clear();
    }

    /// Clears all entries from the value cache.
    ///
    /// This method removes all cached data values from memory, forcing
    /// subsequent reads to reload data from disk or memory tables. This
    /// can help free memory or ensure fresh data is read.
    pub fn clear_value_cache(&mut self) {
        self.value_cache.clear();
    }

    fn apply_compression(&mut self, data: Vec<u8>) -> Result<Vec<u8>, Box<dyn Error>> {
        if self.settings.compressor.config.compression_type == CompressionType::None {
            Ok(data)
        } else {
            let start_time = std::time::Instant::now();
            let original_size = data.len();
            let compressed = self.settings.compressor.compress(&data)?;
            let compression_time = start_time.elapsed().as_millis();
            self.compression_stats.update_compression(
                original_size,
                compressed.len(),
                compression_time
            );
            Ok(compressed)
        }
    }

    fn apply_decompression(&mut self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        if self.settings.compressor.config.compression_type == CompressionType::None {
            Ok(data.to_vec())
        } else {
            let start_time = std::time::Instant::now();
            let decompressed = self.settings.compressor.decompress(data)?;
            let decompression_time = start_time.elapsed().as_millis();
            self.compression_stats.update_decompression(decompression_time);
            Ok(decompressed)
        }
    }

    /// Creates and loads a Tree from the default database path.
    ///
    /// This will scan the default database directory for existing SSTable files
    /// and load them into the tree structure.
    ///
    /// # Returns
    /// A new Tree instance loaded with existing data
    pub fn load() -> Self {
        let mut tree = Self::new();
        tree.load_tree();
        tree
    }

    /// Creates and loads a Tree from a specific database path.
    ///
    /// # Arguments
    /// * `path` - The database directory path to load from
    ///
    /// # Returns
    /// A new Tree instance loaded with existing data from the specified path
    pub fn load_with_path(path: &str) -> Self {
        let mut tree = Self::new();
        tree.settings.db_path = PathBuf::from(path);
        tree.load_tree();
        tree
    }

    /// Creates and loads a Tree with custom settings.
    ///
    /// # Arguments
    /// * `settings` - TreeSettings configuration
    ///
    /// # Returns
    /// A new Tree instance loaded with existing data using the specified settings
    pub fn load_with_settings(settings: TreeSettings) -> Self {
        let mut tree = Self::new();
        tree.settings = settings.clone();
        tree.load_tree();
        tree
    }

    fn load_tree(&mut self) {
        let db_path: PathBuf = if self.settings.db_path.as_os_str().is_empty() {
            PathBuf::from(DEFAULT_DB_PATH)
        } else {
            self.settings.db_path.clone()
        };
        if !db_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&db_path) {
                panic!("Error creating folder for database: {}", e);
            }
        }

        self.settings.db_path = db_path.clone();
        self.mem_table.clear();
        self.immutable_mem_tables.clear();
        self.ss_tables.clear();

        match std::fs::read_dir(&db_path) {
            Ok(entries) => {
                let mut sstable_files_set = HashSet::new();

                for entry in entries {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_file() {
                            if let Some(extension) = path.extension() {
                                if extension == "sst" {
                                    if let Some(filename) = path.file_name() {
                                        if filename.to_string_lossy().starts_with("sstable_") {
                                            sstable_files_set.insert(path);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let mut sstable_files = Vec::new();
                for path in sstable_files_set {
                    sstable_files.push(path);
                }

                sstable_files.sort_by_key(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .and_then(|name| {
                            if name.starts_with("sstable_") && name.ends_with(".sst") {
                                name[8..name.len() - 4].parse::<u64>().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0)
                });

                for sstable_path in sstable_files {
                    if self.validate_sstable(&sstable_path) {
                        self.ss_tables.push(sstable_path.clone());
                    } else {
                        warn!("Damaged SSTable file: {:?}", sstable_path);
                    }
                }

                self.cleanup_expired();
            }
            Err(e) => {
                log::error!("Error reading database folder: {}", e);
            }
        }
    }

    /// Stores a typed value in the tree without TTL.
    ///
    /// The value is automatically serialized using bincode.
    ///
    /// # Arguments
    /// * `key` - The string key to store the value under
    /// * `value` - The value to store (must implement Encode trait)
    ///
    /// # Type Parameters
    /// * `T` - The type of value to store, must implement bincode::Encode
    pub fn put_typed<T>(&mut self, key: &str, value: &T)
    where
        T: Encode,
    {
        self.put_typed_with_ttl_optional::<T>(key, value, None);
    }

    /// Stores a typed value in the tree with a TTL.
    ///
    /// The value will automatically expire after the specified duration.
    ///
    /// # Arguments
    /// * `key` - The string key to store the value under
    /// * `value` - The value to store (must implement Encode trait)
    /// * `ttl` - Time-to-live duration for the value
    ///
    /// # Type Parameters
    /// * `T` - The type of value to store, must implement bincode::Encode
    pub fn put_typed_with_ttl<T>(&mut self, key: &str, value: &T, ttl: Duration)
    where
        T: Encode,
    {
        self.put_typed_with_ttl_optional::<T>(key, value, Some(ttl));
    }

    fn put_typed_with_ttl_optional<T>(&mut self, key: &str, value: &T, ttl: Option<Duration>)
    where
        T: Encode,
    {
        let key_bytes = key.as_bytes().to_vec();
        match bincode::encode_to_vec(value, self.settings.bincode_config) {
            Ok(serialized) => self.put_with_ttl(key_bytes, serialized, ttl),
            Err(e) => log::error!("Error serializing value for key '{}': {}", key, e),
        }
    }

    /// Stores raw bytes in the tree without TTL.
    ///
    /// # Arguments
    /// * `key` - The key as a byte vector
    /// * `value` - The value as a byte vector
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.put_with_ttl(key, value, None);
    }

    /// Stores raw bytes in the tree with optional TTL.
    ///
    /// # Arguments
    /// * `key` - The key as a byte vector
    /// * `value` - The value as a byte vector
    /// * `ttl` - Optional time-to-live duration
    pub fn put_with_ttl(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        self.put_to_tree(key, value, ttl);
    }

    /// Stores raw bytes directly in the tree structure.
    ///
    /// This is the core storage method that handles memory table overflow
    /// and triggers flushing when necessary.
    ///
    /// # Arguments
    /// * `key` - The key as a byte vector
    /// * `value` - The value as a byte vector
    /// * `ttl` - Optional time-to-live duration
    pub fn put_to_tree(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let data = match self.apply_compression(value.to_vec()) {
            Ok(data) => data,
            Err(e) => {
                log::error!("Error compressing value for key '{:?}': {}", key, e);
                return;
            }
        };
        self.mem_table.insert(key, DataValue::new(data, ttl));
        if self.mem_table.len() > self.settings.mem_table_max_size {
            self.flush_mem_table();
        }
    }

    /// Retrieves and deserializes a typed value from the tree.
    ///
    /// # Arguments
    /// * `key` - The string key to look up
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize to, must implement bincode::Decode
    ///
    /// # Returns
    /// `Some(T)` if the key exists and can be deserialized, `None` otherwise
    pub fn get_typed<T>(&mut self, key: &str) -> Option<T>
    where
        T: bincode::Decode<()>,
    {
        let key_bytes = key.as_bytes();
        let value_bytes = self.get(key_bytes)?;
        match bincode::decode_from_slice(&value_bytes, self.settings.bincode_config) {
            Ok((decoded, _)) => Some(decoded),
            Err(e) => {
                log::error!("Error deserializing value for key '{}': {}", key, e);
                None
            }
        }
    }

    /// Retrieves multiple typed values from the tree in a single operation.
    ///
    /// This method allows efficient batch retrieval of multiple keys, returning
    /// the deserialized values in the same order as the input keys. For each key,
    /// the result will be `Some(T)` if the key exists and can be deserialized,
    /// or `None` if the key doesn't exist, has expired, or deserialization fails.
    ///
    /// # Arguments
    /// * `keys` - A vector of string keys to retrieve
    ///
    /// # Type Parameters
    /// * `T` - The type to deserialize values to, must implement `bincode::Decode`
    ///
    /// # Returns
    /// A `Vec<Option<T>>` where each element corresponds to the key at the same
    /// index in the input vector. `Some(T)` if the key exists and is valid,
    /// `None` otherwise.
    ///
    /// # Performance
    /// This method is more efficient than calling `get_typed` multiple times
    /// for the same keys, as it can optimize lookups and reduce repeated
    /// deserialization overhead.
    /// # Error Handling
    /// If deserialization fails for any key, that entry will be `None` in the
    /// result vector, and an error will be logged. The operation continues
    /// for the remaining keys.
    ///
    /// # See Also
    /// - [`get_typed`] - For retrieving a single typed value
    /// - [`get_vec`] - For retrieving multiple raw byte values
    pub fn multi_get_typed<T>(&mut self, keys: Vec<&str>) -> Vec<Option<T>>
    where
        T: bincode::Decode<()>,
    {
        keys.into_iter()
            .map(|key| self.get_typed::<T>(key))
            .collect()
    }

    /// Retrieves multiple raw byte values from the tree in a single operation.
    ///
    /// This method allows efficient batch retrieval of multiple keys, returning
    /// the raw byte values in the same order as the input keys.
    ///
    /// # Arguments
    /// * `keys` - A vector of byte slice keys to retrieve
    ///
    /// # Returns
    /// A `Vec<Option<Vec<u8>>>` where each element corresponds to the key at the
    /// same index in the input vector. `Some(Vec<u8>)` if the key exists and is valid,
    /// `None` otherwise.
    pub fn multi_get(&mut self, keys: Vec<&[u8]>) -> Vec<Option<Vec<u8>>> {
        keys.into_iter().map(|key| self.get(key)).collect()
    }

    /// Retrieves raw bytes from the tree.
    ///
    /// Searches through memory tables and SSTable files in order.
    /// Returns None if the key doesn't exist or has expired.
    ///
    /// # Arguments
    /// * `key` - The key to look up as a byte slice
    ///
    /// # Returns
    /// `Some(Vec<u8>)` if the key exists and is valid, `None` otherwise
    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.mem_table.get(key) {
            if !value.is_expired() {
                let data = value.get_data().to_vec();
                return self.decompress_value_data(data);
            }
        }

        for immutable_mem_table in self.immutable_mem_tables.iter().rev() {
            if let Some(value) = immutable_mem_table.get(key) {
                if !value.is_expired() {
                    let data = value.get_data().to_vec();
                    return self.decompress_value_data(data);
                }
            }
        }

        let sstables = self.ss_tables.clone();
        for sst_path in sstables.iter().rev() {
            if let Some(value) = self.read_key_from_sstable(sst_path, key) {
                if !value.is_expired() {
                    let data = value.get_data().to_vec();
                    return self.decompress_value_data(data);
                }
            }
        }

        None
    }

    /// Gets a mutable reference to a value in the memory table.
    ///
    /// Only works for values currently in the active memory table.
    ///
    /// # Arguments
    /// * `key` - The key to look up as a byte slice
    ///
    /// # Returns
    /// `Some(&mut DataValue)` if the key exists in the memory table, `None` otherwise
    pub fn get_mut(&mut self, key: &[u8]) -> Option<&mut DataValue> {
        self.mem_table.get_mut(key)
    }

    /// Deletes a key from the tree by inserting a tombstone.
    ///
    /// # Arguments
    /// * `key` - The key to delete as a byte slice
    ///
    /// # Returns
    /// `true` if the key existed and was marked for deletion, `false` otherwise
    pub fn delete(&mut self, key: &[u8]) -> bool {
        if self.contains_key(key) {
            self.mem_table.insert(key.to_vec(), DataValue::tombstone());
            true
        } else {
            false
        }
    }

    /// Clears all entries from the active memory table.
    ///
    /// This method removes all key-value pairs from the current memory table,
    /// but does not affect immutable memory tables or SSTable files on disk.
    /// The data in immutable memory tables and SSTable files remains intact.
    ///
    /// # Note
    /// This operation only affects the in-memory data structure and does not
    /// trigger any disk I/O operations or compaction processes.
    pub fn clear_mem_table(&mut self) {
        self.mem_table.clear();
    }

    /// Clears all data from the tree, including memory tables and SSTable references.
    ///
    /// This method performs a complete reset of the tree's in-memory state by:
    /// - Clearing the active memory table
    /// - Clearing all immutable memory tables
    /// - Clearing the SSTable file references
    ///
    /// # Warning
    /// This method does NOT delete the actual SSTable files from disk. It only
    /// removes the references to them from the tree's internal state. The files
    /// will remain on disk and can be reloaded by calling `load_tree()` or
    /// creating a new tree instance with the same database path.
    /// # See Also
    /// - [`clear_mem_table`] - For clearing only the active memory table
    /// - [`load_tree`] - For reloading data from disk after clearing
    pub fn clear_all(&mut self) {
        self.mem_table.clear();
        self.immutable_mem_tables.clear();
        self.ss_tables.clear();
    }

    /// Removes expired entries from memory tables.
    ///
    /// This method scans through all memory tables and removes entries
    /// that have exceeded their TTL.
    pub fn cleanup_expired(&mut self) {
        let expired_keys: Vec<Vec<u8>> = self
            .mem_table
            .iter()
            .filter(|(_, value)| value.is_expired())
            .map(|(key, _)| key.clone())
            .collect();

        for key in expired_keys {
            self.mem_table.remove(&key);
        }

        for mem_table in &mut self.immutable_mem_tables {
            let expired_keys: Vec<Vec<u8>> = mem_table
                .iter()
                .filter(|(_, value)| value.is_expired())
                .map(|(key, _)| key.clone())
                .collect();

            for key in expired_keys {
                mem_table.remove(&key);
            }
        }
    }

    /// Checks if a key exists in the tree.
    ///
    /// # Arguments
    /// * `key` - The key to check as a byte slice
    ///
    /// # Returns
    /// `true` if the key exists and is valid, `false` otherwise
    pub fn contains_key(&mut self, key: &[u8]) -> bool {
        self.get(key).is_some()
    }

    /// Returns the number of active (non-expired) entries in the tree.
    ///
    /// This includes entries in memory tables and SSTable files.
    /// Note: This operation may be expensive as it scans all SSTable files.
    ///
    /// # Returns
    /// The total number of active entries
    pub fn len(&self) -> usize {
        let mem_count = self
            .mem_table
            .values()
            .filter(|value| !value.is_expired())
            .count();

        let immutable_count: usize = self
            .immutable_mem_tables
            .iter()
            .map(|table| table.values().filter(|value| !value.is_expired()).count())
            .sum();

        let sstable_count: usize = self
            .ss_tables
            .iter()
            .map(|table_path| {
                let table = self.load_sstable(table_path);
                table
                    .values()
                    .filter(|value| !value.is_expired() || !value.is_tombstone())
                    .count()
            })
            .sum();

        mem_count + immutable_count + sstable_count
    }

    /// Gets the remaining TTL for a key.
    ///
    /// # Arguments
    /// * `key` - The key to check as a byte slice
    ///
    /// # Returns
    /// `Some(Duration)` if the key exists and has a TTL, `None` otherwise
    pub fn get_ttl(&self, key: &[u8]) -> Option<Duration> {
        if let Some(value) = self.mem_table.get(key) {
            if !value.is_expired() {
                if let Some(expires_at) = value.expires_at {
                    if let Ok(remaining) = expires_at.duration_since(SystemTime::now()) {
                        return Some(remaining);
                    }
                }
            }
        }
        None
    }

    /// Updates the TTL for an existing key.
    ///
    /// Only works for keys currently in the active memory table.
    ///
    /// # Arguments
    /// * `key` - The key to update as a byte slice
    /// * `new_ttl` - The new TTL duration, or None to remove expiration
    ///
    /// # Returns
    /// `true` if the key was found and updated, `false` otherwise
    pub fn update_ttl(&mut self, key: &[u8], new_ttl: Option<Duration>) -> bool {
        if let Some(mut value) = self.mem_table.remove(key) {
            if !value.is_expired() {
                value.expires_at = new_ttl.map(|duration| SystemTime::now() + duration);
                self.mem_table.insert(key.to_vec(), value);
                return true;
            }
        }
        false
    }

    /// Flushes the current memory table to disk.
    ///
    /// This forces all data in the active memory table to be written
    /// to an SSTable file on disk.
    pub fn flush(&mut self) {
        if !self.mem_table.is_empty() {
            self.flush_mem_table();
        }
    }

    fn flush_mem_table(&mut self) {
        let immutable = std::mem::take(&mut self.mem_table);
        self.immutable_mem_tables.push_back(immutable);
        self.compact();
    }

    fn compact(&mut self) {
        if self.immutable_mem_tables.is_empty() {
            return;
        }

        let immutable_table = match self.immutable_mem_tables.pop_front() {
            Some(table) => table,
            None => return,
        };

        let new_sstable_path = match self.write_sstable(&immutable_table) {
            Ok(path) => path,
            Err(e) => {
                log::error!("Error writing SSTable: {}", e);
                return;
            }       
        };
        self.ss_tables.push(new_sstable_path.clone());

        if self.ss_tables.len() > 2 {
            self.merge_sstables();
        }
    }

    fn decompress_value_data(&mut self, data: Vec<u8>) -> Option<Vec<u8>> {
        match self.apply_decompression(&data) {
            Ok(decompressed) => Some(decompressed),
            Err(e) => {
                log::error!("Error decompressing value: {}", e);
                None
            }
        }
    }

}