use crate::config::{BINCODE_CONFIG, DEFAULT_BLOOM_FILTER_ERROR_PROBABILITY, DEFAULT_DB_PATH, DEFAULT_MEM_TABLE_SIZE};
use crate::tree::{CompressionConfig, Compressor};
use std::path::PathBuf;

/// Configuration settings for the LSM Tree database.
///
/// `TreeSettings` contains all the configuration options that control the behavior
/// of the LSM Tree database, including storage paths, memory limits, caching options,
/// and compression settings.
///
/// # Fields
///
/// ## Storage Configuration
/// - `db_path`: The filesystem path where the database files will be stored
/// - `bincode_config`: Configuration for the bincode serialization library
///
/// ## Memory Management
/// - `mem_table_max_size`: Maximum number of entries in the memory table before flushing to disk
///
/// ## Bloom Filter Desired Error Probability
/// - `bloom_filter_error_probability`: The desired error probability (eg. 0.05, 0.01)
/// 
/// ## Caching Options
/// - `enable_index_cache`: Whether to enable caching of SSTable indexes in memory
/// - `enable_value_cache`: Whether to enable caching of frequently accessed values
///
/// ## Compression
/// - `compressor`: The compression algorithm and settings to use for data storage
///
/// # Performance Tuning
///
/// ## Memory Table Size
/// Larger memory tables reduce I/O operations but use more RAM
/// 
/// ## Index Cache
/// Caching SSTable indexes improves read performance
///
/// ## Value Cache
/// Caching frequently accessed values significantly improves read performance:
///
/// ## Compression
/// Different compression algorithms offer various trade-offs:
/// - **None**: No compression overhead, larger disk usage
/// - **LZ4**: Fast compression/decompression, moderate compression ratio
/// - **Zstd**: Better compression ratio, moderate speed
/// - **Snappy**: Very fast, good for high-throughput scenarios
#[derive(Clone)]
pub struct TreeSettings {
    pub db_path: PathBuf,
    pub bincode_config: bincode::config::Configuration,
    pub mem_table_max_size: usize,
    pub bloom_filter_error_probability: f64,
    pub enable_bloom_filter_cache: bool,
    pub enable_index_cache: bool,
    pub enable_value_cache: bool,
    pub compressor: Compressor,
}

impl Default for TreeSettings {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from(DEFAULT_DB_PATH),
            bincode_config: BINCODE_CONFIG,
            mem_table_max_size: DEFAULT_MEM_TABLE_SIZE as usize,
            bloom_filter_error_probability: DEFAULT_BLOOM_FILTER_ERROR_PROBABILITY,
            enable_bloom_filter_cache: true,
            enable_index_cache: true,
            enable_value_cache: true,
            compressor: Compressor::new(CompressionConfig::balanced()),
        }
    }
}

/// A builder for creating `TreeSettings` with a fluent API.
///
/// `TreeSettingsBuilder` provides a convenient way to construct `TreeSettings` instances
/// with custom configurations. It uses the builder pattern to allow method chaining
/// and provides sensible defaults for any unspecified options.
///
/// # Builder Pattern
/// This builder follows the standard Rust builder pattern:
/// 1. Create a new builder with `TreeSettingsBuilder::new()`
/// 2. Configure options using the various setter methods
/// 3. Build the final `TreeSettings` with `build()`
///
/// # Default Values
/// Any options not explicitly set will use their default values:
/// - `db_path`: Uses `DEFAULT_DB_PATH` from config
/// - `bincode_config`: Uses `BINCODE_CONFIG` from config  
/// - `mem_table_max_size`: Uses `DEFAULT_MEM_TABLE_SIZE` from config
/// - `bloom_filter_error_probability`: Uses `DEFAULT_BLOOM_FILTER_ERROR_PROBABILITY` from config
/// - `enable_bloom_filter_cache`: `true`
/// - `enable_index_cache`: `true`
/// - `enable_value_cache`: `true`
/// - `compressor`: Uses `CompressionConfig::balanced()`
pub struct TreeSettingsBuilder {
    db_path: Option<PathBuf>,
    bincode_config: Option<bincode::config::Configuration>,
    mem_table_max_size: Option<usize>,
    bloom_filter_error_probability: Option<f64>,
    enable_bloom_filter_cache: Option<bool>,
    enable_index_cache: Option<bool>,
    enable_value_cache: Option<bool>,
    compressor: Option<Compressor>,
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
            bloom_filter_error_probability: None,
            enable_bloom_filter_cache: None,
            enable_index_cache: None,
            enable_value_cache: None,
            compressor: None
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

    /// Sets the bloom filter desired error probability.
    ///
    /// # Arguments
    /// * `percent` - percent of desired error probability
    ///
    /// # Returns
    /// Self for method chaining
    pub fn bloom_filter_error_probability(mut self, percent: f64) -> Self {
        self.bloom_filter_error_probability = Some(percent);
        self
    }

    /// Enables or disables bloom filter caching.
    ///
    /// When bloom filter caching is enabled, the system automatically loads and
    /// stores bloom filters from SSTable files in memory for fast access. This
    /// significantly improves read operation performance by allowing quick
    /// determination of whether a key might be present in a specific SSTable file
    /// without needing to reload the bloom filter from disk repeatedly.
    ///
    /// # Benefits of enabling caching:
    /// - Significant speedup of key search operations
    /// - Reduced disk I/O operations
    /// - More efficient SSTable file filtering during searches
    /// - Improved performance when working with many SSTable files
    ///
    /// # Arguments
    /// * `is_enabled` - `true` to enable bloom filter caching,
    ///                  `false` to disable it
    ///
    /// # Returns
    /// Returns `Self` to enable method chaining
    pub fn bloom_filter_cache(mut self, is_enabled: bool) -> Self {
        self.enable_bloom_filter_cache = Some(is_enabled);
        self
    }

    /// Enables or disables the index cache.
    ///
    /// The index cache stores SSTable index data in memory to speed up key lookups.
    /// When enabled, frequently accessed SSTable indexes are cached in memory,
    /// reducing disk I/O operations during read operations.
    ///
    /// # Arguments
    /// * `is_enabled` - `true` to enable index caching, `false` to disable
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Performance Impact
    /// - **Enabled**: Faster reads, higher memory usage
    /// - **Disabled**: Slower reads, lower memory usage
    ///
    /// # Default
    /// Index cache is enabled by default in most configurations.
    pub fn index_cache(mut self, is_enabled: bool) -> Self {
        self.enable_index_cache = Some(is_enabled);
        self
    }

    /// Enables or disables the value cache.
    ///
    /// The value cache stores recently accessed data values in memory to improve
    /// read performance for frequently accessed keys. This cache operates using
    /// an LRU (Least Recently Used) eviction policy.
    ///
    /// # Arguments
    /// * `is_enabled` - `true` to enable value caching, `false` to disable
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Cache Behavior
    /// - **Cache Hit**: Returns value directly from memory
    /// - **Cache Miss**: Reads from disk and stores in cache
    /// - **Eviction**: Removes least recently used items when memory limit is reached
    ///
    /// # Performance Impact
    /// - **Enabled**: Significantly faster reads for hot data, higher memory usage
    /// - **Disabled**: Consistent read performance, lower memory usage
    ///
    /// # Default
    /// Value cache is enabled by default in most configurations.
    pub fn value_cache(mut self, is_enabled: bool) -> Self {
        self.enable_value_cache = Some(is_enabled);
        self
    }

    /// Sets the compression configuration for the tree.
    ///
    /// This method configures how data is compressed before being written to disk.
    /// Different compression algorithms offer different trade-offs between compression
    /// ratio, speed, and CPU usage.
    ///
    /// # Arguments
    /// * `config` - A `CompressionConfig` instance specifying the compression settings
    ///
    /// # Returns
    /// Self for method chaining
    ///
    /// # Available Compression Types
    /// - **None**: No compression (fastest, largest size)
    /// - **Snappy**: Fast compression with decent ratio
    /// - **Lz4**: Good balance of speed and compression
    /// - **Zstd**: Best compression ratio, slower
    ///
    /// # Default
    /// No compression is used by default.
    pub fn compressor(mut self, config: CompressionConfig) -> Self {
        self.compressor = Some(Compressor::new(config));
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
            db_path: self
                .db_path
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH)),
            bincode_config: self.bincode_config.unwrap_or(BINCODE_CONFIG),
            mem_table_max_size: self
                .mem_table_max_size
                .unwrap_or(DEFAULT_MEM_TABLE_SIZE as usize),
            bloom_filter_error_probability: self.bloom_filter_error_probability
                .unwrap_or(DEFAULT_BLOOM_FILTER_ERROR_PROBABILITY),
            enable_bloom_filter_cache: self.enable_bloom_filter_cache.unwrap_or(true),
            enable_index_cache: self.enable_index_cache.unwrap_or(true),
            enable_value_cache: self.enable_value_cache.unwrap_or(true),
            compressor: self.compressor.unwrap_or(Compressor::new(CompressionConfig::balanced())),
        }
    }
}