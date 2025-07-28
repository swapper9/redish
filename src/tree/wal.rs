use crate::config::{BINCODE_CONFIG, WAL_LOG_NAME, WAL_TEMP_LOG_NAME};
use crate::tree::tree_error::{TreeError, TreeResult};
use crate::{DataValue, Tree};
use crc32fast::Hasher;
use log::info;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

impl Tree {
    pub(crate) fn init_wal(&mut self) -> TreeResult<()> {
        let wal_path = self.settings.db_path.join(WAL_LOG_NAME);
        if !wal_path.exists() {
            if let Some(parent_dir) = wal_path.parent() {
                std::fs::create_dir_all(parent_dir)?;
            }
        }
        let writer = WalWriter::open(&wal_path)
            .map_err(|e| TreeError::wal(format!("Failed to initialize WAL: {}", e)))?;

        self.wal_writer = Some(writer);

        Ok(())
    }

    pub(crate) fn write_to_wal(
        &mut self,
        op: WalOperation,
        key: &[u8],
        data_value_opt: Option<&DataValue>,
    ) -> TreeResult<()> {
        let should_checkpoint = self.should_checkpoint_wal();

        if let Some(ref mut wal_writer) = self.wal_writer {
            wal_writer.write_entry(op, &key, data_value_opt)
                .map_err(|e| TreeError::wal(format!("Failed to write to WAL: {}", e)))?;

            if should_checkpoint {
                wal_writer.write_checkpoint()
                    .map_err(|e| TreeError::wal(format!("Failed to write checkpoint: {}", e)))?;

                self.cleanup_wal_before_checkpoint()?;
            }
        }

        Ok(())
    }

    fn should_checkpoint_wal(&self) -> bool {
        let wal_path = self.settings.db_path.join(WAL_LOG_NAME);
        if let Ok(metadata) = std::fs::metadata(wal_path) {
            metadata.len() > self.settings.wal_max_size
        } else {
            false
        }
    }

    pub(crate) fn recover_from_wal(&mut self) -> TreeResult<()> {
        let wal_path = self.settings.db_path.join(WAL_LOG_NAME);
        if !wal_path.exists() {
            return Ok(());
        }

        let mut reader = WalReader::open(&wal_path)
            .map_err(|e| TreeError::wal(format!("Failed to open WAL for recovery: {}", e)))?;

        let entries = reader.read_entries()
            .map_err(|e| TreeError::wal(format!("Failed to read WAL entries: {}", e)))?;

        let mut last_checkpoint_index = None;
        for (index, (op, _, _)) in entries.iter().enumerate() {
            if matches!(op, WalOperation::Checkpoint) {
                last_checkpoint_index = Some(index);
            }
        }

        let start_index = match last_checkpoint_index {
            Some(checkpoint_idx) => {
                info!("Found checkpoint at index {}, recovering entries after it", checkpoint_idx);
                checkpoint_idx + 1
            }
            None => {
                info!("No checkpoint found in WAL, recovering all {} entries", entries.len());
                0
            }
        };

        let mut recovered_count = 0;
        for (op, key, data_value) in entries.into_iter().skip(start_index) {
            match op {
                WalOperation::Put => {
                    self.mem_table.insert(key, data_value);
                    recovered_count += 1;
                }
                WalOperation::Delete => {
                    self.mem_table.insert(key, DataValue::tombstone());
                    recovered_count += 1;
                }
                WalOperation::Checkpoint => {
                    continue;
                }
            }
        }

        info!("Recovered {} entries from WAL", recovered_count);
        self.init_wal()?;

        Ok(())
    }

    fn cleanup_wal_before_checkpoint(&mut self) -> TreeResult<()> {
        let wal_path = self.settings.db_path.join(WAL_LOG_NAME);
        let temp_wal_path = self.settings.db_path.join(WAL_TEMP_LOG_NAME);

        if !wal_path.exists() {
            return Ok(());
        }

        let mut reader = WalReader::open(&wal_path)
            .map_err(|e| TreeError::wal(format!("Failed to open WAL for cleanup: {}", e)))?;

        let entries = reader.read_entries()
            .map_err(|e| TreeError::wal(format!("Failed to read WAL entries during cleanup: {}", e)))?;

        let mut last_checkpoint_index = None;
        for (index, (op, _, _)) in entries.iter().enumerate() {
            if matches!(op, WalOperation::Checkpoint) {
                last_checkpoint_index = Some(index);
                info!("Found checkpoint in WAL, starting cleanup at index {}", index);
            }
        }

        let checkpoint_index = match last_checkpoint_index {
            Some(index) => {
                info!("Found checkpoint at index {}, recovering entries after it", index);
                index
            },
            None => {
                info!("No checkpoint found in WAL, recovering all {} entries", entries.len());
                return Ok(());
            }
        };

        let mut new_writer = WalWriter::open(&temp_wal_path)
            .map_err(|e| TreeError::wal(format!("Failed to create temporary WAL file: {}", e)))?;

        let mut entries_written = 0;
        for (op, key, data_value) in entries.into_iter().skip(checkpoint_index + 1) {
            new_writer.write_entry(op, &key, Some(&data_value))
                .map_err(|e| {
                    let _ = std::fs::remove_file(&temp_wal_path);
                    TreeError::wal(format!("Failed to write entry to new WAL: {}", e))
                })?;
            entries_written += 1;
        }

        self.wal_writer = None;

        std::fs::rename(&temp_wal_path, &wal_path)
            .map_err(|e| {
                let _ = std::fs::remove_file(&temp_wal_path);
                TreeError::wal(format!("Failed to replace WAL file: {}", e))
            })?;

        self.wal_writer = Some(WalWriter::open(&wal_path)
            .map_err(|e| TreeError::wal(format!("Failed to reinitialize WAL writer after cleanup: {}", e)))?);
        info!("WAL cleanup completed. {} entries retained", entries_written);

        Ok(())
    }
}
pub(crate) enum WalOperation {
    Checkpoint = 1,
    Put = 2,
    Delete = 3,
}

impl WalOperation {
    fn to_u8(&self) -> u8 {
        match self {
            WalOperation::Checkpoint => 1,
            WalOperation::Put => 2,
            WalOperation::Delete => 3,
        }
    }
}

pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write_entry(
        &mut self,
        op: WalOperation,
        key: &[u8],
        data_value: Option<&DataValue>,
    ) -> std::io::Result<()> {
        let mut hasher = Hasher::new();
        hasher.update(&[op.to_u8()]);
        hasher.update(&(key.len() as u32).to_le_bytes());
        hasher.update(key);
        let value_bytes = match data_value {
            Some(dv) => bincode::encode_to_vec(dv, BINCODE_CONFIG).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to serialize DataValue: {}", e),
                )
            })?,
            None => Vec::new(),
        };
        hasher.update(&(value_bytes.len() as u32).to_le_bytes());
        hasher.update(&value_bytes);
        let crc = hasher.finalize();

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&[op.to_u8()])?;
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key)?;
        self.writer
            .write_all(&(value_bytes.len() as u32).to_le_bytes())?;
        self.writer.write_all(&value_bytes)?;

        self.writer.flush()
    }

    pub(crate) fn write_checkpoint(&mut self) -> std::io::Result<()> {
        self.write_entry(WalOperation::Checkpoint, b"", None)?;
        Ok(())
    }
}

pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    fn read_entries(&mut self) -> std::io::Result<Vec<(WalOperation, Vec<u8>, DataValue)>> {
        use std::io::Read;
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
}
