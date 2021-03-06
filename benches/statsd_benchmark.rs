use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::convert::TryInto;

fn parse(
    line: &Bytes,
) -> Result<statsrelay::statsd_proto::Pdu, statsrelay::statsd_proto::ParseError> {
    statsrelay::statsd_proto::Pdu::parse(line.clone())
}

fn criterion_benchmark(c: &mut Criterion) {
    let by = Bytes::from_static(
        b"hello_world.worldworld_i_am_a_pumpkin:3|c|@1.0|#tags:tags,tags:tags,tags:tags,tags:tags",
    );
    c.bench_function("statsd pdu parsing", |b| b.iter(|| parse(black_box(&by))));
    c.bench_function("statsd pdu conversion", |b| {
        b.iter(|| {
            let _: statsrelay::statsd_proto::Owned =
                parse(black_box(&by)).unwrap().try_into().unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
