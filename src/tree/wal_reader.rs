use crate::config::{BINCODE_CONFIG, CHECKPOINT_ENTRY_SIZE};
use crate::tree::wal::WalOperation;
use crate::DataValue;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::Path;

pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    pub(crate) fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    pub(crate) fn read_entries(&mut self) -> std::io::Result<Vec<(WalOperation, Vec<u8>, DataValue)>> {
        use std::io::{Read, Seek, SeekFrom};
        let file_size = self.reader.seek(SeekFrom::End(0))?;
        if file_size == 0 {
            return Ok(Vec::new());
        }
        self.reader.seek(SeekFrom::Start(0))?;

        let mut entries = Vec::new();

        loop {
            let mut crc_buf = [0u8; 4];
            if self.reader.read_exact(&mut crc_buf).is_err() {
                break;
            }

            let mut op_buf = [0u8; 1];
            self.reader.read_exact(&mut op_buf)?;
            let op = match op_buf[0] {
                1 => WalOperation::Checkpoint,
                2 => WalOperation::Put,
                3 => WalOperation::Delete,
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Invalid WAL operation",
                    ))
                }
            };

            let mut key_len_buf = [0u8; 4];
            self.reader.read_exact(&mut key_len_buf)?;
            let key_len = u32::from_le_bytes(key_len_buf) as usize;

            let mut key = vec![0u8; key_len];
            self.reader.read_exact(&mut key)?;

            let mut value_len_buf = [0u8; 4];
            self.reader.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf) as usize;

            let mut value_bytes = vec![0u8; value_len];
            self.reader.read_exact(&mut value_bytes)?;

            let mut hasher = Hasher::new();
            hasher.update(&op_buf);
            hasher.update(&key_len_buf);
            hasher.update(&key);
            hasher.update(&value_len_buf);
            hasher.update(&value_bytes);
            if hasher.finalize() != u32::from_le_bytes(crc_buf) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "WAL operation CRC mismatch",
                ));
            }

            let data_value = if value_bytes.is_empty() {
                match op {
                    WalOperation::Delete => DataValue::tombstone(),
                    WalOperation::Checkpoint => DataValue::checkpoint(),
                    _ => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Empty value for non-empty operation",
                        ))
                    }
                }
            } else {
                bincode::decode_from_slice(&value_bytes, BINCODE_CONFIG)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to deserialize DataValue: {}", e),
                        )
                    })?
                    .0
            };

            entries.push((op, key, data_value));
        }

        Ok(entries)
    }

    pub(crate) fn has_checkpoint_at_end(&mut self) -> std::io::Result<bool> {
        use std::io::{Read, Seek, SeekFrom};

        let file_size = self.reader.seek(SeekFrom::End(0))?;
        if file_size < CHECKPOINT_ENTRY_SIZE as u64 {
            return Ok(false);
        }

        self.reader.seek(SeekFrom::End(-(CHECKPOINT_ENTRY_SIZE as i64)))?;
        let mut buffer = [0u8; CHECKPOINT_ENTRY_SIZE];
        self.reader.read_exact(&mut buffer)?;

        Ok(buffer[4] == WalOperation::Checkpoint.to_u8())
    }
}