use std::error::Error;
use std::fmt;
use std::io::Write;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
    Snappy,
}

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

    pub fn with_level(mut self, level: i32) -> Self {
        self.level = Some(level);
        self
    }

    pub fn with_checksum(mut self, enable: bool) -> Self {
        self.enable_checksum = enable;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn fast() -> Self {
        Self::new(CompressionType::Snappy)
    }

    pub fn balanced() -> Self {
        Self::new(CompressionType::Lz4).with_level(1)
    }

    pub fn best() -> Self {
        Self::new(CompressionType::Zstd).with_level(9)
    }

    pub fn ultra() -> Self {
        Self::new(CompressionType::Zstd).with_level(19)
    }
}

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

    pub fn update_decompression(&mut self, time_ms: u128) {
        self.decompression_operations += 1;
        self.total_decompression_time_ms += time_ms;
    }

    pub fn average_compression_ratio(&self) -> f64 {
        if self.total_original_size > 0 {
            self.total_compressed_size as f64 / self.total_original_size as f64
        } else {
            0.0
        }
    }

    pub fn average_compression_time_ms(&self) -> f64 {
        if self.compression_operations > 0 {
            self.total_compression_time_ms as f64 / self.compression_operations as f64
        } else {
            0.0
        }
    }

    pub fn average_decompression_time_ms(&self) -> f64 {
        if self.decompression_operations > 0 {
            self.total_decompression_time_ms as f64 / self.decompression_operations as f64
        } else {
            0.0
        }
    }

    pub fn compression_ratio_percentage(&self) -> f64 {
        (1.0 - self.average_compression_ratio()) * 100.0
    }

    pub fn bytes_saved(&self) -> usize {
        if self.total_original_size > self.total_compressed_size {
            self.total_original_size - self.total_compressed_size
        } else {
            0
        }
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for CompressionStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Compression Stats: {} operations, {:.2}% compression ratio, {} bytes saved, avg compression: {:.2}ms, avg decompression: {:.2}ms",
            self.total_operations,
            self.compression_ratio_percentage(),
            self.bytes_saved(),
            self.average_compression_time_ms(),
            self.average_decompression_time_ms()
        )
    }
}

#[derive(Clone)]
pub struct Compressor {
    pub config: CompressionConfig,
}

impl Compressor {
    pub fn new(config: CompressionConfig) -> Self {
        Self { config }
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
        let compressed = match self.config.compression_type {
            CompressionType::None => data.to_vec(),
            CompressionType::Lz4 => self.compress_lz4(data)?,
            CompressionType::Zstd => self.compress_zstd(data)?,
            CompressionType::Snappy => self.compress_snappy(data)?,
        };
        Ok(compressed)
    }

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

pub fn compress_with_config(
    data: &[u8],
    config: CompressionConfig,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let compressor = Compressor::new(config);
    compressor.compress(data)
}

pub fn decompress_with_config(
    compressed: &[u8],
    config: CompressionConfig,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let compressor = Compressor::new(config);
    compressor.decompress(compressed)
}
