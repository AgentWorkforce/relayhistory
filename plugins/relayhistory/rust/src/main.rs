use std::io::{Read, Write};
const MAX_INPUT: u64 = 16 * 1024 * 1024;
const MAX_OUTPUT: usize = 32 * 1024 * 1024;
fn main() {
    // The host may terminate sooner. A stuck transport or interactive login cannot
    // leave a helper indefinitely holding an authentication lock.
    std::thread::spawn(|| {
        std::thread::sleep(std::time::Duration::from_secs(300));
        eprintln!("RelayHistory helper deadline exceeded");
        std::process::exit(124);
    });
    if run().is_err() {
        eprintln!("RelayHistory helper protocol failure");
        std::process::exit(1);
    }
}
fn run() -> anyhow::Result<()> {
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_INPUT + 1)
        .read_to_end(&mut input)?;
    anyhow::ensure!(input.len() as u64 <= MAX_INPUT, "request too large");
    let request = serde_json::from_slice(&input)?;
    let output = serde_json::to_vec(&relayhistory_plugin::helper::handle(request))?;
    anyhow::ensure!(
        output.len() <= MAX_OUTPUT,
        "response too large; use replay file output"
    );
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&output)?;
    stdout.write_all(b"\n")?;
    Ok(())
}
