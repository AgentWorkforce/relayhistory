//! Rust command-line entry point. Reusable ingestion lives in ai_hist.
fn main() -> anyhow::Result<()> {
    ai_hist_cli::run()
}
