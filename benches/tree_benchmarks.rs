use bincode::{Decode, Encode};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use redish::tree::Tree;
use std::time::Duration;

#[derive(Debug, Encode, Decode, Clone)]
struct User {
    user_id: u64,
    username: String,
    email: String,
    age: u32,
}

impl User {
    fn new(id: u64) -> Self {
        User {
            user_id: id,
            username: format!("user_{}", id),
            email: format!("user{}@example.com", id),
            age: 25 + (id % 50) as u32,
        }
    }
}

fn setup_tree() -> Tree {
    let temp_dir = std::env::temp_dir().join("redish_bench");
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).ok();
    }
    Tree::load_with_path(temp_dir.to_str().unwrap()).unwrap()
}

fn bench_put_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("put_operations");
    group.measurement_time(Duration::from_secs(20));

    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));

        group.bench_with_input(
            BenchmarkId::new("put_typed", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut tree = setup_tree();
                    for i in 0..size {
                        let user = User::new(i);
                        tree.put_typed::<User>(&format!("user_{}", i), &user).unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("put_raw", size),
            size,
            |b, &size| {
                b.iter(|| {
                    let mut tree = setup_tree();
                    for i in 0..size {
                        let key = format!("key_{}", i).into_bytes();
                        let value = format!("value_{}", i).into_bytes();
                        tree.put(key, value).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_get_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_operations");
    group.measurement_time(Duration::from_secs(10));

    let mut tree = setup_tree();
    for i in 0..10000 {
        let user = User::new(i);
        tree.put_typed::<User>(&format!("user_{}", i), &user).unwrap();
    }

    group.bench_function("get_typed_sequential", |b| {
        b.iter(|| {
            for i in 0..1000 {
                let result = tree.get_typed::<User>(&format!("user_{}", i)).unwrap();
                black_box(result);
            }
        });
    });

    group.bench_function("get_typed_random", |b| {
        use rand::Rng;
        b.iter(|| {
            let mut rng = rand::rng();
            for _ in 0..1000 {
                let i = rng.random_range(0..10000);
                let result = tree.get_typed::<User>(&format!("user_{}", i)).unwrap();
                black_box(result);
            }
        });
    });

    group.finish();
}

fn bench_mixed_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_operations");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("mixed_workload", |b| {
        b.iter(|| {
            let mut tree = setup_tree();
            use rand::Rng;
            let mut rng = rand::rng();

            for i in 0..1000 {
                let operation = rng.random_range(0..3);
                match operation {
                    0 => {
                        let user = User::new(i);
                        tree.put_typed::<User>(&format!("user_{}", i), &user).unwrap();
                    }
                    1 => {
                        let key = format!("user_{}", rng.random_range(0..i.max(1)));
                        let result = tree.get_typed::<User>(&key).unwrap();
                        black_box(result);
                    }
                    2 => {
                        let key = format!("user_{}", rng.random_range(0..i.max(1)));
                        tree.delete(key.as_bytes()).unwrap();
                    }
                    _ => {}
                }
            }
        });
    });

    group.finish();
}

fn bench_ttl_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_operations");
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("put_with_ttl", |b| {
        b.iter(|| {
            let mut tree = setup_tree();
            for i in 0..1000 {
                let user = User::new(i);
                tree.put_typed_with_ttl::<User>(&format!("user_{}", i), &user, Duration::from_secs(60)).unwrap();
            }
        });
    });

    group.bench_function("cleanup_expired", |b| {
        let mut tree = setup_tree();

        // Заполняем данные с коротким TTL
        for i in 0..1000 {
            let user = User::new(i);
            tree.put_typed_with_ttl::<User>(&format!("user_{}", i), &user, Duration::from_millis(1)).unwrap();
        }

        // Ждем истечения TTL
        std::thread::sleep(Duration::from_millis(10));

        b.iter(|| {
            tree.cleanup_expired().unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_put_operations,
    bench_get_operations,
    bench_mixed_operations,
    bench_ttl_operations
);
criterion_main!(benches);