use std::fs;
use std::path::PathBuf;

pub(crate) fn logo() {
    if cfg!(not(debug_assertions)) {
        use log::info;
        info!("______           _  _       _     ");
        info!("| ___ \\         | |(_)     | |    ");
        info!("| |_/ / ___   __| | _  ___ | |__  ");
        info!("|    / / _ \\ / _` || |/ __|| '_ \\ ");
        info!("| |\\ \\|  __/| (_| || |\\__ \\| | | |");
        info!("\\_| \\_|\\___| \\__,_||_||___/|_| |_|");
        info!("Redish v{} started", env!("CARGO_PKG_VERSION"));
    }
}

pub(crate) fn find_last_ss_table_number(directory: &PathBuf) -> Option<usize> {
    fs::read_dir(directory)
        .ok()?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();
            if file_name_str.starts_with("sstable_") && file_name_str.ends_with(".sst") {
                let number_part = &file_name_str[8..file_name_str.len() - 4];
                number_part.parse::<usize>().ok()
            } else {
                None
            }
        })
        .max()
}

// fn extract_number(path: &PathBuf) -> usize {
//     let file_name = path.file_name().unwrap().to_string_lossy();
//     if file_name.starts_with("sstable_") && file_name.ends_with(".sst") {
//         let number_part = &file_name[8..file_name.len() - 4];
//         number_part.parse::<usize>().unwrap_or(0)
//     } else {
//         0
//     }
// }

// pub(crate) fn sort_table_paths_by_number(mut table_paths: Vec<PathBuf>) -> (PathBuf, PathBuf) {
//     table_paths.sort_by(|a, b| {
//         extract_number(a).cmp(&extract_number(b))
//     });
//     (table_paths[0].clone(), table_paths[1].clone())
// }
