use crate::config::{CURRENT_VERSION, FOOTER_MAGIC_NUMBER, FOOTER_SIZE, HEADER_MAGIC_NUMBER};
use crate::tree::tree_error::TreeResult;
use crate::tree::BloomFilter;
use crate::{util, DataValue, Tree};
use crc32fast::Hasher;
use growable_bloom_filter::GrowableBloom;
use log::error;
use std::cmp::PartialEq;
use std::collections::{BTreeMap, BinaryHeap};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

impl Tree {
    pub(crate) fn read_key_from_sstable(
        &mut self,
        path: &PathBuf,
        key: &[u8],
    ) -> Option<DataValue> {
        if self.settings.enable_value_cache {
            if let Some(cached_value) = self.value_cache.get(path, key) {
                if !cached_value.is_expired() {
                    return Some(cached_value);
                } else {
                    self.value_cache.remove(path, key);
                }
            }
        }

        if self.settings.enable_index_cache {
            if let Some(cached_index) = self.index_cache.get(path) {
                if let Some(&offset) = cached_index.get(key) {
                    let file = File::open(path).ok()?;
                    let mut reader = BufReader::new(file);
                    match self.read_data_entry(&mut reader, offset) {
                        Ok(data_value) => {
                            if self.settings.enable_value_cache {
                                self.value_cache.put(
                                    path.clone(),
                                    key.to_vec(),
                                    data_value.clone(),
                                );
                            }
                            return Some(data_value);
                        }
                        Err(e) => {
                            error!("Error reading data entry from SSTable {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        if !self.check_bloom_filter(key, path) {
            return None;
        }

        let file = File::open(path).ok()?;
        let mut reader = BufReader::new(file);

        if self.validate_header(&mut reader).is_err() {
            return None;
        }

        let (index_offset, _) = self.read_footer(&mut reader).ok()?;
        let data_offset = self.find_key_in_index(&mut reader, index_offset, key)?;

        if self.settings.enable_index_cache {
            if let Ok(index) = self.read_index(&mut reader, index_offset) {
                self.index_cache.put(path.clone(), index);
            }
        }

        match self.read_data_entry(&mut reader, data_offset) {
            Ok(data_value) => {
                if self.settings.enable_value_cache {
                    self.value_cache
                        .put(path.clone(), key.to_vec(), data_value.clone());
                }
                Some(data_value)
            }
            Err(e) => {
                error!(
                    "Error reading data entry from SSTable {:?} with offset {:?}: {}",
                    path,
                    data_offset,
                    e
                );
                None
            }
        }
    }

    fn check_bloom_filter(&mut self, key: &[u8], path: &PathBuf) -> bool {
        if self.settings.enable_bloom_filter_cache {
            if let Some(bf) = self.bloom_filters
                .iter()
                .find(|bf| bf.path == *path) {
                return bf.bloom_filter.contains(key)
            }
        }

        match self.load_bloom_filter(path) {
            Ok(bloom_filter) => {
                let contains_key = bloom_filter.contains(key);
                if self.settings.enable_bloom_filter_cache {
                    self.bloom_filters.push(BloomFilter {
                        path: path.clone(),
                        bloom_filter,
                    });
                }
                contains_key
            }
            Err(e) => {
                error!("Error loading SSTable {:?} for bloom filter check: {}", path, e);
                false
            }
        }
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

    fn read_footer(&self, reader: &mut BufReader<File>) -> std::io::Result<(u64, u64)> {
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;

        let mut index_offset_bytes = [0u8; 8];
        reader.read_exact(&mut index_offset_bytes)?;
        let index_offset = u64::from_le_bytes(index_offset_bytes);

        let mut bloom_offset_bytes = [0u8; 8];
        reader.read_exact(&mut bloom_offset_bytes)?;
        let bloom_offset = u64::from_le_bytes(bloom_offset_bytes);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;

        if &magic != FOOTER_MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incorrect footer magic number",
            ));
        }

        Ok((index_offset, bloom_offset))
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

    pub(crate) fn load_sstable(&mut self, path: &PathBuf) -> BTreeMap<Vec<u8>, DataValue> {
        match self.load_sstable_with_bloom_filter(path) {
            Ok((table, bloom_filter)) => {
                if self.settings.enable_bloom_filter_cache {
                    self.bloom_filters.push(BloomFilter {
                        path: path.clone(),
                        bloom_filter,
                    });
                }
                table
            },
            Err(e) => {
                error!("Error loading SSTable {:?}: {}", path, e);
                BTreeMap::new()
            }
        }
    }

    pub(crate) fn load_sstable_with_bloom_filter(
        &self,
        path: &PathBuf,
    ) -> Result<(BTreeMap<Vec<u8>, DataValue>, GrowableBloom), std::io::Error> {
        let mut table = BTreeMap::new();

        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);

                if let Err(e) = self.validate_header(&mut reader) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Wrong header SSTable {:?}: {}", path, e)));
                }

                let (index_offset, bloom_offset) = match self.read_footer(&mut reader) {
                    Ok(offsets) => offsets,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Error reading footer SSTable {:?}: {}", path, e)));
                    }
                };

                let index = match self.read_index(&mut reader, index_offset) {
                    Ok(idx) => idx,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Error reading index SSTable {:?}: {}", path, e)));
                    }
                };

                for (key, offset) in index {
                    if let Ok(value) = self.read_data_entry(&mut reader, offset) {
                        table.insert(key, value);
                    }
                }

                let bloom_filter = match self.read_bloom_filter(&mut reader, bloom_offset) {
                    Ok(bloom_filter) => bloom_filter,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Error reading bloom filter SSTable {:?}: {}", path, e)));
                    }
                };

                Ok((table, bloom_filter))
            }
            Err(e) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Error opening SSTable {:?}: {}", path, e)))
            }
        }
    }

    pub(crate) fn load_bloom_filter(
        &self,
        path: &PathBuf,
    ) -> Result<GrowableBloom, std::io::Error> {
        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);

                if let Err(e) = self.validate_header(&mut reader) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Wrong header SSTable {:?}: {}", path, e)));
                }

                let (_, bloom_offset) = match self.read_footer(&mut reader) {
                    Ok(offsets) => offsets,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Error reading footer SSTable {:?}: {}", path, e)));
                    }
                };

                let bloom_filter = match self.read_bloom_filter(&mut reader, bloom_offset) {
                    Ok(bloom_filter) => bloom_filter,
                    Err(e) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Error reading bloom filter SSTable {:?}: {}", path, e)));
                    }
                };

                Ok(bloom_filter)
            }
            Err(e) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Error opening SSTable {:?}: {}", path, e)))
            }
        }
    }

    pub(crate) fn write_sstable(
        &mut self,
        table: &BTreeMap<Vec<u8>, DataValue>,
    ) -> Result<(PathBuf, GrowableBloom), std::io::Error> {
        let new_sstable_number = match util::find_last_sstable_number(&self.settings.db_path) {
            None => 0,
            Some(number) => number + 1,
        };
        let table_path = self
            .settings
            .db_path
            .join(format!("sstable_{}.sst", new_sstable_number));
        if let Some(parent_dir) = table_path.parent() {
            std::fs::create_dir_all(parent_dir)?;
        }

        let file = File::create(&table_path)?;
        let mut writer = BufWriter::new(file);

        self.write_header(&mut writer)?;

        let mut index = BTreeMap::new();
        let mut bloom_filter =
            GrowableBloom::new(self.settings.bloom_filter_error_probability, table.len());

        for (key, value) in table {
            let offset = writer.stream_position()?;
            self.write_data_entry(&mut writer, key, value)?;
            index.insert(key.clone(), offset);
            bloom_filter.insert(key);
        }

        let index_offset = writer.stream_position()?;
        self.write_index(&mut writer, &index)?;

        let bloom_offset = writer.stream_position()?;
        self.write_bloom_filter(&mut writer, &bloom_filter)?;

        self.write_footer(&mut writer, index_offset, bloom_offset)?;

        writer.flush()?;
        if self.settings.enable_index_cache {
            self.index_cache.put(table_path.clone(), index);
        }
        Ok((table_path, bloom_filter))
    }

    fn write_header(&self, writer: &mut BufWriter<File>) -> std::io::Result<()> {
        writer.write_all(HEADER_MAGIC_NUMBER)?;
        writer.write_all(&CURRENT_VERSION.to_le_bytes())?;
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

    fn write_bloom_filter(
        &self,
        writer: &mut BufWriter<File>,
        bloom_filter: &GrowableBloom,
    ) -> std::io::Result<()> {
        match serde_json::to_vec(bloom_filter) {
            Ok(serialized_data) => {
                let size = serialized_data.len();
                writer.write_all(&(size as u32).to_le_bytes())?;
                writer.write_all(&serialized_data)?;
                Ok(())
            }
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize bloom filter: {}", e),
            )),
        }
    }

    fn read_bloom_filter(
        &self,
        reader: &mut BufReader<File>,
        offset: u64,
    ) -> std::io::Result<GrowableBloom> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut size_bytes = [0u8; 4];
        reader.read_exact(&mut size_bytes)?;
        let size = u32::from_le_bytes(size_bytes) as usize;

        let mut serialized_data = vec![0u8; size];
        reader.read_exact(&mut serialized_data)?;

        match serde_json::from_slice(&serialized_data) {
            Ok(bloom_filter) => Ok(bloom_filter),
            Err(e) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize bloom filter: {}", e),
            )),
        }
    }

    fn write_footer(
        &self,
        writer: &mut BufWriter<File>,
        index_offset: u64,
        bloom_filter_offset: u64,
    ) -> std::io::Result<()> {
        writer.write_all(&index_offset.to_le_bytes())?;
        writer.write_all(&bloom_filter_offset.to_le_bytes())?;
        writer.write_all(FOOTER_MAGIC_NUMBER)?;
        Ok(())
    }

    pub(crate) fn merge_sstables(&mut self) -> TreeResult<()> {
        let tables_to_merge_count = std::cmp::min(self.ss_tables.len(), 3);
        if tables_to_merge_count < 2 {
            return Ok(());
        }

        let tables_to_merge: Vec<PathBuf> =
            self.ss_tables.drain(0..tables_to_merge_count).collect();

        let mut table_data: Vec<BTreeMap<Vec<u8>, DataValue>> =
            Vec::with_capacity(tables_to_merge.len());
        for table_path in &tables_to_merge {
            table_data.push(self.load_sstable(table_path));
        }

        let mut iterators: Vec<_> = table_data
            .iter()
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

        if self.settings.enable_index_cache {
            for path in &tables_to_merge {
                self.index_cache.remove(path);
                self.index_cache.lru_queue.retain(|p| !p.eq(path));
                self.index_cache.remove(path);
            }
        }
        if self.settings.enable_value_cache {
            for path in &tables_to_merge {
                self.value_cache.invalidate_sstable(path);
            }
        }

        match self.write_sstable(&merged_data) {
            Ok((path, bloom_filter)) => {
                self.ss_tables.push(path.clone());
                if self.settings.enable_bloom_filter_cache {
                    self.bloom_filters.push(BloomFilter { path, bloom_filter })
                }
            }
            Err(e) => {
                error!("Error writing merged SSTable: {}", e);
                return Ok(());
            }
        };

        for path in tables_to_merge {
            if let Err(e) = std::fs::remove_file(&path) {
                error!("Error deleting old SSTable {:?}: {}", path, e);
            }
            self.ss_tables.retain(|p| p != &path);
            self.bloom_filters.retain(|bf| bf.path != path);
        }

        if let Err(e) = self.rename_sstables_after_merge() {
            error!("Error renaming SSTable files: {}", e);
            return Ok(());
        }

        self.remove_obsolete_wal_segments();

        Ok(())
    }

    fn rename_sstables_after_merge(&mut self) -> std::io::Result<()> {
        let mut sstables_with_numbers: Vec<(usize, PathBuf)> = Vec::new();

        for path in &self.ss_tables {
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                if file_name.starts_with("sstable_") && file_name.ends_with(".sst") {
                    let number_part = &file_name[8..file_name.len() - 4];
                    if let Ok(number) = number_part.parse::<usize>() {
                        sstables_with_numbers.push((number, path.clone()));
                    }
                }
            }
        }

        if sstables_with_numbers.is_empty() {
            return Ok(());
        }

        if sstables_with_numbers[0].0 > self.ss_tables.len() {
            let mut updated_paths = Vec::new();
            for (new_index, (_, old_path)) in sstables_with_numbers.into_iter().enumerate() {
                let new_name = format!("sstable_{}.sst", new_index);
                let new_path = old_path.parent().unwrap().join(&new_name);

                if old_path != new_path {
                    std::fs::rename(&old_path, &new_path)?;

                    if self.settings.enable_index_cache {
                        if let Some(cached_index) = self.index_cache.remove(&old_path) {
                            self.index_cache.put(new_path.clone(), cached_index.clone());
                        }
                    }

                    if self.settings.enable_value_cache {
                        self.value_cache.rename_sstable(&old_path, &new_path);
                    }

                    for bloom_filter in &mut self.bloom_filters {
                        if bloom_filter.path == old_path {
                            bloom_filter.path = new_path.clone();
                        }
                    }
                }
                updated_paths.push(new_path);
            }
            self.ss_tables = updated_paths;
        }

        Ok(())
    }

    fn find_key_in_index(
        &self,
        reader: &mut BufReader<File>,
        index_offset: u64,
        key: &[u8],
    ) -> Option<u64> {
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
                    return Some(*offset);
                }
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Greater => right = mid,
            }
        }

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

    pub(crate) fn validate_sstable(&self, path: &PathBuf) -> bool {
        match File::open(path) {
            Ok(file) => {
                let mut reader = BufReader::new(file);
                if self.validate_header(&mut reader).is_err() {
                    error!("Error validating header SSTable : {:?}", path);
                    return false;
                }
                if self.read_footer(&mut reader).is_err() {
                    error!("Error validating footer SSTable {:?}", path);
                    return false;
                }
                true
            }
            Err(e) => {
                error!("Error opening SSTable {:?}: {}", path, e);
                false
            }
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
