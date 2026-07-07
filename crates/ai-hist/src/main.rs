//! Thin binary wrapper. All logic lives in the `ai_hist_cli` library so it can
//! also be driven in-process (e.g. by the napi binding) rather than only via
//! the CLI.

fn main() -> anyhow::Result<()> {
    ai_hist_cli::run()
}
