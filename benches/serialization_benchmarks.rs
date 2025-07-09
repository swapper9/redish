use bincode::{Decode, Encode};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

#[derive(Debug, Encode, Decode, Clone)]
struct SimpleStruct {
    id: u64,
    name: String,
}

#[derive(Debug, Encode, Decode, Clone)]
struct ComplexStruct {
    id: u64,
    name: String,
    values: Vec<i32>,
    metadata: std::collections::HashMap<String, String>,
}

fn bench_serialization(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialization");

    let simple = SimpleStruct {
        id: 12345,
        name: "Test User".to_string(),
    };

    let complex = ComplexStruct {
        id: 12345,
        name: "Test User".to_string(),
        values: (0..100).collect(),
        metadata: {
            let mut map = std::collections::HashMap::new();
            for i in 0..10 {
                map.insert(format!("key_{}", i), format!("value_{}", i));
            }
            map
        },
    };

    group.bench_function("serialize_simple", |b| {
        b.iter(|| {
            let encoded = bincode::encode_to_vec(&simple, bincode::config::standard()).unwrap();
            black_box(encoded);
        });
    });

    group.bench_function("deserialize_simple", |b| {
        let encoded = bincode::encode_to_vec(&simple, bincode::config::standard()).unwrap();
        b.iter(|| {
            let decoded: SimpleStruct = bincode::decode_from_slice(&encoded, bincode::config::standard()).unwrap().0;
            black_box(decoded);
        });
    });

    group.bench_function("serialize_complex", |b| {
        b.iter(|| {
            let encoded = bincode::encode_to_vec(&complex, bincode::config::standard()).unwrap();
            black_box(encoded);
        });
    });

    group.bench_function("deserialize_complex", |b| {
        let encoded = bincode::encode_to_vec(&complex, bincode::config::standard()).unwrap();
        b.iter(|| {
            let decoded: ComplexStruct = bincode::decode_from_slice(&encoded, bincode::config::standard()).unwrap().0;
            black_box(decoded);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_serialization);
criterion_main!(benches);