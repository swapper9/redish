use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use redish::tree::Tree;

fn bench_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_usage");

    group.bench_function("memory_growth", |b| {
        b.iter(|| {
            let mut tree = Tree::new().unwrap();

            for i in 0..10000 {
                let key = format!("key_{}", i);
                let value = format!("value_{}", i);
                tree.put_typed(&key, &value).unwrap();

                if i % 1000 == 0 {
                    black_box(tree.len());
                }
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_memory_usage);
criterion_main!(benches);