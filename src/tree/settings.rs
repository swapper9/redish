use std::path::PathBuf;
use crate::config::{DEFAULT_DB_PATH, BINCODE_CONFIG, DEFAULT_MEM_TABLE_SIZE};
use crate::tree::{CompressionConfig, Compressor};

#[derive(Clone)]
pub struct TreeSettings {
    pub db_path: PathBuf,
    pub bincode_config: bincode::config::Configuration,
    pub mem_table_max_size: usize,
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
            enable_index_cache: true,
            enable_value_cache: true,
            compressor: Compressor::new(CompressionConfig::default()),
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
            db_path: self
                .db_path
                .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH)),
            bincode_config: self.bincode_config.unwrap_or(BINCODE_CONFIG),
            mem_table_max_size: self
                .mem_table_max_size
                .unwrap_or(DEFAULT_MEM_TABLE_SIZE as usize),
            enable_index_cache: true,
            enable_value_cache: true,
            compressor: Compressor::new(CompressionConfig::balanced()),
        }
    }
}