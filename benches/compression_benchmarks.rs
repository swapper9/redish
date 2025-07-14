use criterion::{black_box, criterion_group, criterion_main, Criterion};
use redish::tree::{CompressionConfig, CompressionType, Compressor};

fn benchmark_compression(c: &mut Criterion) {
    let test_data = b"This is a test string that should compress well.".repeat(100);

    let mut group = c.benchmark_group("compression");

    for compression_type in [CompressionType::Snappy, CompressionType::Lz4, CompressionType::Zstd] {
        let config = CompressionConfig::new(compression_type);
        let compressor = Compressor::new(config);

        group.bench_function(format!("{:?}", compression_type), |b| {
            b.iter(|| {
                let compressed = compressor.compress(black_box(&test_data)).unwrap();
                compressor.decompress(black_box(&compressed)).unwrap()
            })
        });
    }

    group.finish();
}

criterion_group!(benches, benchmark_compression);
criterion_main!(benches);
