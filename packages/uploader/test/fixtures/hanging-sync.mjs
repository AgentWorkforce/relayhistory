// Stands in for a capture that never finishes and ignores SIGTERM.
process.on("SIGTERM", () => {});
setInterval(() => {}, 1_000);
