#[cfg(test)]
mod test;

use crate::config::{BINCODE_CONFIG, CURRENT_VERSION, DEFAULT_DB_PATH, DEFAULT_MEM_TABLE_SIZE, FOOTER_MAGIC_NUMBER, FOOTER_SIZE, HEADER_MAGIC_NUMBER};
use crate::{logger, util};
use bincode::{Decode, Encode};
use crc32fast::Hasher;
use log::{debug, error, warn};
use once_cell::sync::Lazy;
use std::collections::{BTreeMap, BinaryHeap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

static INIT: Lazy<()> = Lazy::new(|| {
    logger::init_logger().expect("Logger was not initialized!");
});

#[derive(Clone, Debug, Encode, Decode, Eq, PartialEq)]
pub struct DataValue {
    data: Vec<u8>,
    expires_at: Option<SystemTime>,
    created_at: SystemTime,
    is_tombstone: bool,
}

impl DataValue {
    /// Creates a new DataValue with the specified data and optional TTL.
    ///
    /// # Arguments
    /// * `data` - The raw data to store as bytes
    /// * `ttl` - Optional time-to-live duration. If None, the value never expires
    ///
    /// # Returns
    /// A new DataValue instance with creation timestamp and calculated expiration time
    pub fn new(data: Vec<u8>, ttl: Option<Duration>) -> Self {
        let created_at = SystemTime::now();
        let expires_at = ttl.map(|duration| created_at + duration);

        Self {
            data,
            expires_at,
            created_at,
            is_tombstone: false,
        }
    }

    /// Checks if the stored data is empty.
    ///
    /// # Returns
    /// `true` if the data vector is empty, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Checks if the value has expired based on its TTL.
    ///
    /// # Returns
    /// `true` if the value has expired, `false` if it's still valid or has no expiration
    pub fn is_expired(&self) -> bool {
        if let Some(expiry) = self.expires_at {
            SystemTime::now() > expiry
        } else {
            false
        }
    }

    /// Returns a reference to the stored data.
    ///
    /// # Returns
    /// A byte slice reference to the stored data
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    /// Creates a tombstone marker for deletion.
    ///
    /// A tombstone is a special marker that indicates a key has been deleted.
    /// It's used internally for proper deletion semantics in LSM trees.
    ///
    /// # Returns
    /// A new DataValue instance marked as a tombstone
    pub fn tombstone() -> Self {
        Self {
            data: Vec::new(),
            expires_at: None,
            created_at: SystemTime::now(),
            is_tombstone: true,
        }
    }

    /// Checks if this value is a tombstone (deletion marker).
    ///
    /// # Returns
    /// `true` if this is a tombstone, `false` otherwise
    pub fn is_tombstone(&self) -> bool {
        self.is_tombstone
    }
}

#[derive(Clone)]
pub struct TreeSettings {
    db_path: PathBuf,
    bincode_config: bincode::config::Configuration,
    mem_table_max_size: usize,
}

impl Default for TreeSettings {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            bincode_config: BINCODE_CONFIG,
            mem_table_max_size: DEFAULT_MEM_TABLE_SIZE as usize,
        }
    }
}

impl TreeSettings {
    /// Creates a new TreeSettings instance with default values.
    ///
    /// # Returns
    /// A new TreeSettings with default database path, bincode configuration, and memory table size
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the database path for the tree.
    ///
    /// # Arguments
    /// * `path` - A path that can be converted to PathBuf
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_db_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.db_path = path.into();
        self
    }

    /// Sets the bincode configuration for serialization.
    ///
    /// # Arguments
    /// * `config` - The bincode configuration to use
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_bincode_config(mut self, config: bincode::config::Configuration) -> Self {
        self.bincode_config = config;
        self
    }

    /// Sets the maximum size for the memory table.
    ///
    /// # Arguments
    /// * `size` - Maximum number of entries in the memory table before flushing
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_mem_table_max_size(mut self, size: usize) -> Self {
        self.mem_table_max_size = size;
        self
    }
}

pub struct TreeSettingsBuilder {
    db_path: Option<PathBuf>,
    bincode_config: Option<bincode::config::Configuration>,
    mem_table_max_size: Option<usize>,
}

impl TreeSettingsBuilder {
    /// Creates a new TreeSettingsBuilder instance.
    ///
    /// # Returns
    /// A new builder with all fields set to None
    pub fn new() -> Self {
        Self {
            db_path: None,
            bincode_config: None,
            mem_table_max_size: None,
        }
    }

    /// Sets the database path.
    ///
    /// # Arguments
    /// * `path` - A path that can be converted to PathBuf
    ///
    /// # Returns
    /// Self for method chaining
    pub fn db_path<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.db_path = Some(path.into());
        self
    }

    /// Sets the bincode configuration.
    ///
    /// # Arguments
    /// * `config` - The bincode configuration to use
    ///
    /// # Returns
    /// Self for method chaining
    pub fn bincode_config(mut self, config: bincode::config::Configuration) -> Self {
        self.bincode_config = Some(config);
        self
    }

    /// Sets the maximum memory table size.
    ///
    /// # Arguments
    /// * `size` - Maximum number of entries in the memory table
    ///
    /// # Returns
    /// Self for method chaining
    pub fn mem_table_max_size(mut self, size: usize) -> Self {
        self.mem_table_max_size = Some(size);
        self
    }

    /// Builds the TreeSettings from the configured options.
    ///
    /// Any unset options will use their default values.
    ///
    /// # Returns
    /// A new TreeSettings instance
    pub fn build(self) -> TreeSettings {
        TreeSettings {
            db_path: self.db_path.unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH)),
            bincode_config: self.bincode_config.unwrap_or(BINCODE_CONFIG),
            mem_table_max_size: self.mem_table_max_size.unwrap_or(DEFAULT_MEM_TABLE_SIZE as usize),
        }
    }
}

// Структура SSTable Header {
//     magic: [u8; 4],        // "SSTB" - magic number
//     version: u32,          // версия формата
//     compression: u8,       // тип сжатия (0 = нет, 1 = lz4, 2 = zstd)
//     checksum_type: u8,     // тип чексуммы (0 = нет, 1 = crc32)
//     reserved: [u8; 6],     // резерв для будущих расширений
// }

// SSTable structure:
// [Header: 16 bytes] [Data Blocks] [Index Block] [Footer]
// [Data Block] :
// [key_len: u32] [key: Vec<u8>] [data_len: u32] [data: Vec<u8>] [checksum: u32]
// Index: [index_num_entries: u32] [index_key_len: u32] [index_key: Vec<u8>] [offset_to_data_entry: u64]
// Footer: [index_offset: u64] [footer_magic: u32]
pub struct Tree {
    mem_table: BTreeMap<Vec<u8>, DataValue>,
    immutable_mem_tables: VecDeque<BTreeMap<Vec<u8>, DataValue>>,
    ss_tables: Vec<PathBuf>,
    settings: TreeSettings,
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
        util::logo();
        Self {
            mem_table: BTreeMap::new(),
            immutable_mem_tables: VecDeque::new(),
            ss_tables: Vec::new(),
            settings: TreeSettings {
                db_path: PathBuf::from(path),
                ..TreeSettings::default()
            }
        }
    }

    /// Creates a new Tree with custom settings.
    ///
    /// # Arguments
    /// * `settings` - TreeSettings configuration
    ///
    /// # Returns
    /// A new Tree instance with the specified settings
    pub fn new_with_settings(settings: TreeSettings) -> Self {
        util::logo();
        Self {
            mem_table: BTreeMap::new(),
            immutable_mem_tables: VecDeque::new(),
            ss_tables: Vec::new(),
            settings,
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
            debug!("Database folder not exist, creating: {:?}",db_path);
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
                                name[8..name.len()-4].parse::<u64>().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0)
                });

                debug!("Sorted SSTable files:");
                for (i, file) in sstable_files.iter().enumerate() {
                    debug!("  {}: {:?}", i, file);
                }

                for sstable_path in sstable_files {
                    if self.validate_sstable(&sstable_path) {
                        self.ss_tables.push(sstable_path.clone());
                        debug!("Loaded SSTable: {:?}", sstable_path);
                    } else {
                        warn!("Damaged SSTable file: {:?}", sstable_path);
                    }
                }

                debug!("Loaded {} SSTable files", self.ss_tables.len());

                self.cleanup_expired();
            }
            Err(e) => {
                log::error!("Error reading database folder: {}", e);
            }
        }
    }

    fn validate_sstable(&self, path: &PathBuf) -> bool {
        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                if self.validate_header(&mut reader).is_err() {
                    log::error!("Error validating header SSTable : {:?}", path);
                    return false;
                }
                if self.read_footer(&mut reader).is_err() {
                    log::error!("Error validating footer SSTable {:?}", path);
                    return false;
                }
                true
            }
            Err(e) => {
                log::error!("Error opening SSTable {:?}: {}", path, e);
                false
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
            Err(e) => log::error!("Error serializing value for key '{}': {}", key, e)
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
        let data_value = DataValue::new(value, ttl);
        self.mem_table.insert(key, data_value);
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
    pub fn get_typed<T>(&self, key: &str) -> Option<T>
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
    pub fn get_vec_typed<T>(&self, keys: Vec<&str>) -> Vec<Option<T>>
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
    pub fn get_vec(&self, keys: Vec<&[u8]>) -> Vec<Option<Vec<u8>>> {
        keys.into_iter()
            .map(|key| self.get(key))
            .collect()
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
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(value) = self.mem_table.get(key) {
            if !value.is_expired() {
                return Some(value.get_data().to_vec());
            }
        }

        for immutable_mem_table in self.immutable_mem_tables.iter().rev() {
            if let Some(value) = immutable_mem_table.get(key) {
                if !value.is_expired() {
                    return Some(value.get_data().to_vec());
                }
            }
        }

        for sst_path in self.ss_tables.iter().rev() {
            if let Some(value) = self.read_key_from_ss_table(sst_path, key) {
                if !value.is_expired() {
                    return Some(value.get_data().to_vec());
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
    pub fn contains_key(&self, key: &[u8]) -> bool {
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
            .map(|table| table.values()
                .filter(|value| !value.is_expired()).count())
            .sum();

        let ss_table_count: usize = self
            .ss_tables
            .iter()
            .map(|table_path| {
                let table = self.load_ss_table(table_path);
                table.values()
                    .filter(|value| !value.is_expired() || !value.is_tombstone()).count()
            })
            .sum();

        mem_count + immutable_count + ss_table_count
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

        let new_ss_table_path = self.write_ss_table(&immutable_table);
        self.ss_tables.push(new_ss_table_path.clone());
        debug!("Compacted immutable mem table to SSTable: {:?}", new_ss_table_path);
        debug!("Current SSTables: {:?}", self.ss_tables);

        if self.ss_tables.len() > 2 {
            self.merge_ss_tables();
        }
    }

    fn load_ss_table(&self, path: &PathBuf) -> BTreeMap<Vec<u8>, DataValue> {
        let mut table = BTreeMap::new();

        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);

                if let Err(e) = self.validate_header(&mut reader) {
                    log::error!("Wrong header SSTable {:?}: {}", path, e);
                    return table;
                }

                let index_offset = match self.read_footer(&mut reader) {
                    Ok(offsets) => offsets,
                    Err(e) => {
                        log::error!("Error reading footer SSTable {:?}: {}", path, e);
                        return table;
                    }
                };

                let index = match self.read_index(&mut reader, index_offset) {
                    Ok(idx) => idx,
                    Err(e) => {
                        log::error!("Error reading index SSTable {:?}: {}", path, e);
                        return table;
                    }
                };

                for (key, offset) in index {
                    if let Ok(value) = self.read_data_entry(&mut reader, offset) {
                        table.insert(key, value);
                    }
                }
            }
            Err(e) => {
                log::error!("Error opening SSTable {:?}: {}", path, e);
            }
        }

        table
    }

    fn write_ss_table(&self, table: &BTreeMap<Vec<u8>, DataValue>) -> PathBuf {
        let new_ss_table_number = match util::find_last_ss_table_number(&self.settings.db_path) {
            None => 0,
            Some(number) => number + 1,
        };
        let table_path = self.settings.db_path
            .join(format!("sstable_{}.sst", new_ss_table_number));
        let file = File::create(&table_path).unwrap();
        let mut writer = BufWriter::new(file);

        self.write_header(&mut writer).unwrap();

        //let data_start = writer.stream_position().unwrap();
        let mut index = BTreeMap::new();
        //let mut bloom_keys = Vec::new();
        //let mut bloom_filter: BloomFilter = BloomFilter::with_rate(0.01, DEFAULT_MEM_TABLE_SIZE);

        for (key, value) in table {
            let offset = writer.stream_position().unwrap();
            self.write_data_entry(&mut writer, key, value).unwrap();

            index.insert(key.clone(), offset);
            //bloom_keys.push(key.clone());
        }

        // 3. Записываем индекс
        let index_offset = writer.stream_position().unwrap();
        self.write_index(&mut writer, &index).unwrap();

        //let bloom_offset = writer.stream_position().unwrap();
        //self.write_bloom_filter(&mut writer, &bloom_filter).unwrap();

        //self.write_footer(&mut writer, index_offset, bloom_offset).unwrap();
        self.write_footer(&mut writer, index_offset).unwrap();

        writer.flush().unwrap();
        table_path
    }

    fn write_header(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_all(HEADER_MAGIC_NUMBER)?; // magic
        writer.write_all(&CURRENT_VERSION.to_le_bytes())?; // version
        writer.write_all(&[0u8; 8])?; // compression, checksum_type, reserved
        Ok(())
    }

    fn write_data_entry(
        &self,
        writer: &mut BufWriter<File>,
        key: &[u8],
        value: &DataValue,
    ) -> std::io::Result<()> {
        let value_bytes = bincode::encode_to_vec(value, self.settings.bincode_config).unwrap();

        writer.write_all(&(key.len() as u32).to_le_bytes())?;
        writer.write_all(key)?;

        writer.write_all(&(value_bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&value_bytes)?;

        let mut hasher = Hasher::new();
        hasher.update(key);
        hasher.update(&value_bytes);
        let checksum = hasher.finalize();
        writer.write_all(&checksum.to_le_bytes())?;

        Ok(())
    }

    fn write_index(
        &self,
        writer: &mut BufWriter<File>,
        index: &BTreeMap<Vec<u8>, u64>,
    ) -> std::io::Result<()> {
        writer.write_all(&(index.len() as u32).to_le_bytes())?;

        for (index_key, offset) in index {
            writer.write_all(&(index_key.len() as u32).to_le_bytes())?;
            writer.write_all(index_key)?;
            writer.write_all(&offset.to_le_bytes())?;
        }
        Ok(())
    }

    fn write_footer(&self, writer: &mut BufWriter<File>, index_offset: u64) -> std::io::Result<()> {
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(FOOTER_MAGIC_NUMBER)?;
        Ok(())
    }

    //TODO:  Incremental Compaction, Parallel Compaction

    fn merge_ss_tables(&mut self) {
        let tables_to_merge_count = std::cmp::min(self.ss_tables.len(), 3);
        if tables_to_merge_count < 2 {
            return;
        }

        let tables_to_merge: Vec<PathBuf> =
            self.ss_tables.drain(0..tables_to_merge_count).collect();
        debug!("Merging SSTables: {:?}", tables_to_merge);

        let mut table_data: Vec<BTreeMap<Vec<u8>, DataValue>> =
            Vec::with_capacity(tables_to_merge.len());
        for table_path in &tables_to_merge {
            table_data.push(self.load_ss_table(table_path));
        }

        let mut iterators: Vec<_> = table_data.iter()
            .map(|table| table.iter())
            .collect();

        let mut min_heap = BinaryHeap::new();

        for (idx, iterator) in iterators.iter_mut().enumerate() {
            if let Some((key, value)) = iterator.next() {
                min_heap.push(HeapEntry {
                    key: key.clone(),
                    value: value.clone(),
                    table_index: idx,
                });
            }
        }

        let mut merged_data = BTreeMap::new();
        let mut last_key: Option<Vec<u8>> = None;

        while let Some(entry) = min_heap.pop() {
            if entry.value.is_empty() || entry.value.is_tombstone {
                continue;
            }

            let HeapEntry {
                key,
                value,
                table_index,
            } = entry;

            if let Some(ref last) = last_key {
                if *last == key {
                    if let Some((next_key, next_value)) = iterators[table_index].next() {
                        min_heap.push(HeapEntry {
                            key: next_key.clone(),
                            value: next_value.clone(),
                            table_index,
                        });
                    }
                    continue;
                }
            }

            last_key = Some(key.clone());
            merged_data.insert(key, value);
            if let Some((next_key, next_value)) = iterators[table_index].next() {
                min_heap.push(HeapEntry {
                    key: next_key.clone(),
                    value: next_value.clone(),
                    table_index,
                });
            }
        }

        let new_table_path = self.write_ss_table(&merged_data);
        self.ss_tables.push(new_table_path.clone());

        for path in tables_to_merge {
            if let Err(e) = std::fs::remove_file(&path) {
                log::error!("Error deleting old SSTable {:?}: {}", path, e);
            }
            self.ss_tables.retain(|p| p != &path);
        }
    }

    fn read_key_from_ss_table(&self, path: &PathBuf, key: &[u8]) -> Option<DataValue> {
        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);

        if self.validate_header(&mut reader).is_err() {
            return None;
        }

        let index_offset = self.read_footer(&mut reader).ok()?;

        // if !self.check_bloom_filter_safe(&mut reader, bloom_offset, key) {
        //     return None;
        // }

        let data_offset = self.find_key_in_index(&mut reader, index_offset, key)?;

        self.read_data_entry(&mut reader, data_offset)
            .ok()
    }

    fn validate_header(&self, reader: &mut BufReader<File>) -> std::io::Result<()> {
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if &magic != HEADER_MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incorrect header magic number",
            ));
        }

        let mut version = [0u8; 4];
        reader.read_exact(&mut version)?;
        let version = u32::from_le_bytes(version);

        if version != CURRENT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported version: {}", version),
            ));
        }

        // Skipping other header bytes, as they are reserved for now
        let mut reserved = [0u8; 8];
        reader.read_exact(&mut reserved)?;

        Ok(())
    }

    fn read_footer(&self, reader: &mut BufReader<File>) -> std::io::Result<u64> {
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let mut index_offset_bytes = [0u8; 8];
        reader.read_exact(&mut index_offset_bytes)?;
        let index_offset = u64::from_le_bytes(index_offset_bytes);

        // let mut bloom_offset_bytes = [0u8; 8];
        // reader.read_exact(&mut bloom_offset_bytes)?;
        // let bloom_offset = u64::from_le_bytes(bloom_offset_bytes);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if &magic != FOOTER_MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incorrect footer magic number",
            ));
        }

        Ok(index_offset)
    }

    fn read_index(
        &self,
        reader: &mut BufReader<File>,
        offset: u64,
    ) -> std::io::Result<BTreeMap<Vec<u8>, u64>> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut count_bytes = [0u8; 4];
        reader.read_exact(&mut count_bytes)?;
        let count = u32::from_le_bytes(count_bytes);

        let mut index = BTreeMap::new();

        for _ in 0..count {
            let mut key_len_bytes = [0u8; 4];
            reader.read_exact(&mut key_len_bytes)?;
            let key_len = u32::from_le_bytes(key_len_bytes) as usize;

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            let mut offset_bytes = [0u8; 8];
            reader.read_exact(&mut offset_bytes)?;
            let data_offset = u64::from_le_bytes(offset_bytes);

            index.insert(key, data_offset);
        }

        Ok(index)
    }

    fn find_key_in_index(&self, reader: &mut BufReader<File>, index_offset: u64, key: &[u8]) -> Option<u64> {
        reader.seek(SeekFrom::Start(index_offset)).ok()?;

        let mut index_num_entries_count_bytes = [0u8; 4];
        reader.read_exact(&mut index_num_entries_count_bytes).ok()?;
        let index_num_entries = u32::from_le_bytes(index_num_entries_count_bytes);

        let mut entries = Vec::with_capacity(index_num_entries as usize);

        for index_entry in 0..index_num_entries {
            let mut index_key_len_bytes = [0u8; 4];
            if reader.read_exact(&mut index_key_len_bytes).is_err() {
                error!("Error reading key len for entry {}", index_entry);
                return None;
            }
            let index_key_len = u32::from_le_bytes(index_key_len_bytes) as usize;

            let mut index_key = vec![0u8; index_key_len];
            if reader.read_exact(&mut index_key).is_err() {
                error!("Error reading key for entry {}", index_entry);
                return None;
            }

            let mut data_entry_offset_bytes = [0u8; 8];
            if reader.read_exact(&mut data_entry_offset_bytes).is_err() {
                error!("Error reading offset for entry {}", index_entry);
                return None;
            }
            let data_entry_offset = u64::from_le_bytes(data_entry_offset_bytes);

            entries.push((index_key, data_entry_offset));
        }

        let mut left = 0;
        let mut right = entries.len();

        while left < right {
            let mid = left + (right - left) / 2;
            let (index_key, offset) = &entries[mid];

            match index_key.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => {
                    debug!("Key found in index: {:?}", String::from_utf8_lossy(key));
                    return Some(*offset);
                }
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => {
                    right = mid
                }
            }
        }

        debug!("Key not found in index: {:?}", String::from_utf8_lossy(key));
        None
    }

    fn read_data_entry(
        &self,
        reader: &mut BufReader<File>,
        offset: u64,
    ) -> std::io::Result<DataValue> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut key_len_bytes = [0u8; 4];
        reader.read_exact(&mut key_len_bytes)?;
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;

        reader.seek(SeekFrom::Current(key_len as i64))?;

        let mut value_len_bytes = [0u8; 4];
        reader.read_exact(&mut value_len_bytes)?;
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;

        let mut value_bytes = vec![0u8; value_len];
        reader.read_exact(&mut value_bytes)?;

        match bincode::decode_from_slice(&value_bytes, self.settings.bincode_config) {
            Ok((decoded, _)) => Ok(decoded),
            Err(e) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Deserialization error: {}", e),
                )),
        }
    }

}

#[derive(Debug, Eq)]
struct HeapEntry {
    key: Vec<u8>,
    value: DataValue,
    table_index: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // reverse order
        other.key.cmp(&self.key)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
