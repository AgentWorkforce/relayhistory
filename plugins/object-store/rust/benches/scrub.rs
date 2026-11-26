//! Scrub throughput on a realistic raw chunk, and on the pathological input
//! behind the reference's documented O(n²) incident.
//!
//! `cargo bench --manifest-path plugins/object-store/rust/Cargo.toml --bench scrub`

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use relayhistory_object_store::scrub::{scrub_chunk, scrub_text};

/// ~4 MiB of Claude-Code-shaped JSONL: prompts, tool results with paths, and
/// the occasional secret, which is the advisory chunk size from the spec.
fn realistic_chunk() -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0u64;
    while out.len() < 4 * 1024 * 1024 {
        let record = serde_json::json!({
            "type": if i.is_multiple_of(3) { "user" } else { "assistant" },
            "uuid": format!("0192f0c4-7f00-7000-8000-{i:012}"),
            "cwd": "/Users/alice/Projects/relayhistory",
            "message": {
                "content": [
                    {"type": "text", "text": format!(
                        "Run the tests in /Users/alice/Projects/relayhistory/crates/ai-hist and \
                         fix the failure in ingest.rs line {i}. The build log says error[E0308] \
                         mismatched types; see https://github.com/AgentWorkforce/relayhistory/pull/{i}."
                    )},
                    {"type": "tool_result", "content": if i.is_multiple_of(50) {
                        "export OPENAI_API_KEY=sk-abcdefghijklmnopqrstuvwxyz0123456789".to_string()
                    } else {
                        "test result: ok. 412 passed; 0 failed; finished in 3.21s".repeat(8)
                    }},
                ]
            },
            "usage": {"input_tokens": 1200 + i, "output_tokens": 300},
        });
        out.extend_from_slice(&serde_json::to_vec(&record).unwrap());
        out.push(b'\n');
        i += 1;
    }
    out
}

fn bench(c: &mut Criterion) {
    let chunk = realistic_chunk();
    let mut g = c.benchmark_group("scrub_chunk");
    g.throughput(Throughput::Bytes(chunk.len() as u64));
    g.sample_size(10);
    g.bench_function(BenchmarkId::new("jsonl", "4MiB"), |b| {
        b.iter(|| scrub_chunk(&chunk))
    });
    g.finish();

    let pathological = "A".repeat(128 * 1024);
    let mut g = c.benchmark_group("scrub_text_redos");
    g.throughput(Throughput::Bytes(pathological.len() as u64));
    g.sample_size(10);
    g.bench_function("AAAA_128KiB", |b| b.iter(|| scrub_text(&pathological)));
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
