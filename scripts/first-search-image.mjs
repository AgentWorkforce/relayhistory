// Preserve locally built images, which may have no registry digest.
export function resolveImage(run, image) {
  const inspectArgs = ['image', 'inspect', image, '--format',
    '{{if .RepoDigests}}{{index .RepoDigests 0}}{{else}}{{.Id}}{{end}}'];
  try {
    return run('docker', inspectArgs).trim();
  } catch {
    run('docker', ['pull', image], { stdio: 'inherit' });
    return run('docker', inspectArgs).trim();
  }
}
