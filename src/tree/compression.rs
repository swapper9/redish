use std::error::Error;
use std::fmt;
use std::io::Write;

/// Compression algorithms supported by the storage engine.
///
/// Each algorithm provides different trade-offs between compression ratio,
/// speed, and CPU usage. Choose the appropriate algorithm based on your
/// performance requirements and data characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
    Snappy,
}

/// Configuration for compression settings.
///
/// This structure allows fine-tuning of compression behavior including
/// algorithm selection, compression level, checksums, and buffer sizes.
///
/// # Examples
/// ```rust
/// use redish::tree::{CompressionConfig, CompressionType};
///
/// // Basic configuration
/// let config = CompressionConfig::new(CompressionType::Lz4);
///
/// // Advanced configuration
/// let config = CompressionConfig::new(CompressionType::Zstd)
///     .with_level(9)
///     .with_checksum(true)
///     .with_buffer_size(8192);
///
/// // Predefined configurations
/// let fast_config = CompressionConfig::fast();      // Snappy
/// let balanced_config = CompressionConfig::balanced(); // LZ4 level 1
/// let best_config = CompressionConfig::best();      // Zstd level 9
/// ```
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub compression_type: CompressionType,
    pub level: Option<i32>,
    pub enable_checksum: bool,
    pub buffer_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::None,
            level: None,
            enable_checksum: false,
            buffer_size: 4096,
        }
    }
}

impl CompressionConfig {
    /// Creates a new compression configuration with the specified algorithm.
    ///
    /// Default values are automatically selected based on the algorithm:
    /// - **Zstd**: Level 3, checksums enabled
    /// - **LZ4**: Level 1, checksums enabled
    /// - **Snappy/None**: No level, checksums enabled
    ///
    /// # Arguments
    /// * `compression_type` - The compression algorithm to use
    ///
    /// # Returns
    /// A new `CompressionConfig` with algorithm-appropriate defaults
    ///
    /// # Examples
    /// ```rust
    /// use redish::tree::{CompressionConfig, CompressionType};
    /// let config = CompressionConfig::new(CompressionType::Zstd);
    /// // Results in: Zstd level 3, checksums enabled, 4KB buffer
    /// ```
    pub fn new(compression_type: CompressionType) -> Self {
        Self {
            compression_type,
            level: match compression_type {
                CompressionType::Zstd => Some(3),
                CompressionType::Lz4 => Some(1),
                _ => None,
            },
            enable_checksum: true,
            buffer_size: 4096,
        }
    }

    /// Sets the compression level.
    ///
    /// Different algorithms support different level ranges:
    /// - **LZ4**: 1-9 (1=fastest, 9=best compression)
    /// - **Zstd**: 1-22 (1=fastest, 22=best compression, 19+=ultra mode)
    /// - **Snappy**: Level ignored (always uses default)
    ///
    /// # Arguments
    /// * `level` - The compression level to use
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_level(mut self, level: i32) -> Self {
        self.level = Some(level);
        self
    }

    /// Enables or disables checksum validation.
    ///
    /// Checksums provide data integrity verification at the cost of:
    /// - Slightly larger compressed data
    /// - Additional CPU overhead during compression/decompression
    /// - Protection against data corruption
    ///
    /// # Arguments
    /// * `enable` - Whether to enable checksum validation
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_checksum(mut self, enable: bool) -> Self {
        self.enable_checksum = enable;
        self
    }

    /// Sets the buffer size for streaming operations.
    ///
    /// Larger buffers can improve compression ratio and performance for
    /// large data sets at the cost of memory usage. The threshold determines
    /// when to use streaming vs. single-shot compression.
    ///
    /// # Arguments
    /// * `size` - Buffer size in bytes
    ///
    /// # Returns
    /// Self for method chaining
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Creates a configuration optimized for speed.
    ///
    /// Uses Snappy compression which provides the fastest compression and
    /// decompression performance with reasonable compression ratios.
    ///
    /// **Characteristics:**
    /// - Algorithm: Snappy
    /// - Speed: Excellent
    /// - Compression ratio: Good
    /// - CPU usage: Very low
    ///
    /// # Returns
    /// A `CompressionConfig` optimized for speed
    pub fn fast() -> Self {
        Self::new(CompressionType::Snappy)
    }

    /// Creates a configuration with balanced speed and compression.
    ///
    /// Uses LZ4 compression at level 1, providing a good balance between
    /// compression speed and compression ratio.
    ///
    /// **Characteristics:**
    /// - Algorithm: LZ4 level 1
    /// - Speed: Very good
    /// - Compression ratio: Very good
    /// - CPU usage: Low
    ///
    /// # Returns
    /// A `CompressionConfig` with balanced performance
    pub fn balanced() -> Self {
        Self::new(CompressionType::Lz4).with_level(1)
    }

    /// Creates a configuration optimized for compression ratio.
    ///
    /// Uses Zstd compression at level 9, providing excellent compression
    /// ratios at the cost of increased CPU usage.
    ///
    /// **Characteristics:**
    /// - Algorithm: Zstd level 9
    /// - Speed: Good
    /// - Compression ratio: Excellent
    /// - CPU usage: Moderate
    ///
    /// # Returns
    /// A `CompressionConfig` optimized for compression ratio
    pub fn best() -> Self {
        Self::new(CompressionType::Zstd).with_level(9)
    }

    /// Creates a configuration with maximum compression.
    ///
    /// Uses Zstd compression at level 19 (ultra mode), providing the best
    /// possible compression ratio at the cost of significantly increased
    /// CPU usage and compression time.
    ///
    /// **Characteristics:**
    /// - Algorithm: Zstd level 19 (ultra mode)
    /// - Speed: Slower
    /// - Compression ratio: Maximum
    /// - CPU usage: High
    ///
    /// # Returns
    /// A `CompressionConfig` with maximum compression
    pub fn ultra() -> Self {
        Self::new(CompressionType::Zstd).with_level(19)
    }
}

/// Statistics tracking compression operations and performance.
///
/// This structure provides comprehensive metrics about compression operations
/// including ratios, timing, and efficiency measurements. All statistics
/// are cumulative and can be reset using the `reset()` method.
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub total_operations: usize,
    pub total_original_size: usize,
    pub total_compressed_size: usize,
    pub total_compression_time_ms: u128,
    pub total_decompression_time_ms: u128,
    pub compression_operations: usize,
    pub decompression_operations: usize,
    pub min_compression_ratio: f64,
    pub max_compression_ratio: f64,
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self {
            total_operations: 0,
            total_original_size: 0,
            total_compressed_size: 0,
            total_compression_time_ms: 0,
            total_decompression_time_ms: 0,
            compression_operations: 0,
            decompression_operations: 0,
            min_compression_ratio: f64::INFINITY,
            max_compression_ratio: 0.0,
        }
    }
}

impl CompressionStats {

    /// Updates statistics with a new compression operation.
    ///
    /// This method should be called after each compression operation to
    /// maintain accurate statistics.
    ///
    /// # Arguments
    /// * `original_size` - Size of the original data in bytes
    /// * `compressed_size` - Size of the compressed data in bytes
    /// * `time_ms` - Time taken for the operation in milliseconds
    ///
    pub fn update_compression(
        &mut self,
        original_size: usize,
        compressed_size: usize,
        time_ms: u128,
    ) {
        self.total_operations += 1;
        self.compression_operations += 1;
        self.total_original_size += original_size;
        self.total_compressed_size += compressed_size;
        self.total_compression_time_ms += time_ms;

        if original_size > 0 {
            let ratio = compressed_size as f64 / original_size as f64;
            if self.min_compression_ratio == f64::INFINITY {
                self.min_compression_ratio = ratio;
            } else {
                self.min_compression_ratio = self.min_compression_ratio.min(ratio);
            }
            self.max_compression_ratio = self.max_compression_ratio.max(ratio);
        }
    }

    /// Updates statistics with a new decompression operation.
    ///
    /// # Arguments
    /// * `time_ms` - Time taken for the decompression operation in milliseconds
    pub fn update_decompression(&mut self, time_ms: u128) {
        self.decompression_operations += 1;
        self.total_decompression_time_ms += time_ms;
    }

    /// Calculates the average compression ratio.
    ///
    /// The compression ratio is the size of compressed data divided by the
    /// size of original data. Lower values indicate better compression.
    ///
    /// # Returns
    /// Average compression ratio (0.0 to 1.0+)
    pub fn average_compression_ratio(&self) -> f64 {
        if self.total_original_size > 0 {
            self.total_compressed_size as f64 / self.total_original_size as f64
        } else {
            0.0
        }
    }

    /// Calculates the average time spent on compression operations.
    ///
    /// # Returns
    /// Average compression time in milliseconds
    pub fn average_compression_time_ms(&self) -> f64 {
        if self.compression_operations > 0 {
            self.total_compression_time_ms as f64 / self.compression_operations as f64
        } else {
            0.0
        }
    }

    /// Calculates the average time spent on decompression operations.
    ///
    /// # Returns
    /// Average decompression time in milliseconds
    pub fn average_decompression_time_ms(&self) -> f64 {
        if self.decompression_operations > 0 {
            self.total_decompression_time_ms as f64 / self.decompression_operations as f64
        } else {
            0.0
        }
    }

    /// Calculates the compression ratio as a percentage.
    ///
    /// This represents the percentage of space saved by compression.
    /// Higher values indicate better compression efficiency.
    ///
    /// # Returns
    /// Compression ratio as a percentage (0.0 to 100.0)
    pub fn compression_ratio_percentage(&self) -> f64 {
        (1.0 - self.average_compression_ratio()) * 100.0
    }

    /// Resets all statistics to their default values.
    ///
    /// This clears all accumulated statistics and resets counters to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for CompressionStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Compression Stats: {} operations, {:.2}% compression ratio, avg compression: {:.2}ms, avg decompression: {:.2}ms",
            self.total_operations,
            self.compression_ratio_percentage(),
            self.average_compression_time_ms(),
            self.average_decompression_time_ms()
        )
    }
}

/// A compression engine that handles data compression and decompression.
///
/// The `Compressor` provides a high-level interface for compressing and
/// decompressing data using various algorithms. It maintains configuration
/// state and provides consistent behavior across different compression types.
///
/// # Examples
/// ```rust
/// use redish::tree::{Compressor, CompressionConfig, CompressionType};
///
/// let config = CompressionConfig::new(CompressionType::Zstd);
/// let compressor = Compressor::new(config);
///
/// let data = b"Hello, World!";
/// let compressed = compressor.compress(data).unwrap();
/// let decompressed = compressor.decompress(&compressed).unwrap();
/// assert_eq!(data, &decompressed[..]);
/// ```
#[derive(Clone)]
pub struct Compressor {
    pub config: CompressionConfig,
}

impl Compressor {

    /// Creates a new compressor with the specified configuration.
    ///
    /// # Arguments
    /// * `config` - The compression configuration to use
    ///
    /// # Returns
    /// A new `Compressor` instance
    ///
    /// # Examples
    /// ```rust
    /// let config = CompressionConfig::balanced();
    /// let compressor = Compressor::new(config);
    /// ```
    pub fn new(config: CompressionConfig) -> Self {
        Self { config }
    }

    /// Compresses the provided data using the configured algorithm.
    ///
    /// The compression behavior depends on the configuration:
    /// - **None**: Returns data unchanged
    /// - **LZ4**: Fast compression with good ratio
    /// - **Zstd**: Configurable compression with excellent ratios
    /// - **Snappy**: Very fast compression with moderate ratio
    ///
    /// # Arguments
    /// * `data` - The data to compress
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - The compressed data
    /// * `Err(Box<dyn Error>)` - If compression fails
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let compressed = match self.config.compression_type {
            CompressionType::None => data.to_vec(),
            CompressionType::Lz4 => self.compress_lz4(data)?,
            CompressionType::Zstd => self.compress_zstd(data)?,
            CompressionType::Snappy => self.compress_snappy(data)?,
        };
        Ok(compressed)
    }

    /// Decompresses the provided data using the configured algorithm.
    ///
    /// The decompression algorithm must match the one used for compression.
    /// The compressor automatically handles algorithm-specific decompression
    /// parameters and streaming when necessary.
    ///
    /// # Arguments
    /// * `compressed` - The compressed data to decompress
    ///
    /// # Returns
    /// * `Ok(Vec<u8>)` - The decompressed data
    /// * `Err(Box<dyn Error>)` - If decompression fails
    pub fn decompress(&self, compressed: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let decompressed = match self.config.compression_type {
            CompressionType::None => compressed.to_vec(),
            CompressionType::Lz4 => self.decompress_lz4(compressed)?,
            CompressionType::Zstd => self.decompress_zstd(compressed)?,
            CompressionType::Snappy => self.decompress_snappy(compressed)?,
        };
        Ok(decompressed)
    }

    fn compress_lz4(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        use lz4::block::{compress, CompressionMode};

        let compressed = compress(data, Some(CompressionMode::DEFAULT), true)?;
        Ok(compressed)
    }

    fn decompress_lz4(&self, compressed: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        use lz4::block::decompress;

        let decompressed = decompress(compressed, None)?;
        Ok(decompressed)
    }

    fn compress_zstd(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        use zstd::stream::{encode_all, Encoder};

        let level = self.config.level.unwrap_or(3);

        if data.len() > self.config.buffer_size {
            let mut encoder = Encoder::new(Vec::new(), level)?;
            encoder.include_checksum(self.config.enable_checksum)?;
            encoder.write_all(data)?;
            encoder.finish()
        } else {
            encode_all(data, level)
        }
        .map_err(|e| e.into())
    }

    fn decompress_zstd(&self, compressed: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        use zstd::stream::{decode_all, Decoder};

        if compressed.len() > self.config.buffer_size {
            let mut decoder = Decoder::new(compressed)?;
            let mut decompressed = Vec::new();
            std::io::copy(&mut decoder, &mut decompressed)?;
            Ok(decompressed)
        } else {
            decode_all(compressed)
        }
        .map_err(|e| e.into())
    }

    fn compress_snappy(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        use snap::raw::Encoder;

        let mut encoder = Encoder::new();
        encoder.compress_vec(data).map_err(|e| e.into())
    }

    fn decompress_snappy(&self, compressed: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        use snap::raw::Decoder;

        let mut decoder = Decoder::new();
        decoder.decompress_vec(compressed).map_err(|e| e.into())
    }
}
