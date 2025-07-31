use crate::config::BINCODE_CONFIG;
use crate::tree::wal::WalOperation;
use crate::DataValue;
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    pub(crate) fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub(crate) fn write_entry(
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
        self.write_entry(WalOperation::Checkpoint, b"CHCKPT", None)?;
        Ok(())
    }
}
