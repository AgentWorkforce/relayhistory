//! Hermes' process-level resolver; no context-local agent override is observable.
use super::*;
pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    resolve_with(home, use_env, cfg!(windows), |key| std::env::var_os(key))
}
pub(super) fn resolve_with(
    home: &Path,
    use_env: bool,
    windows: bool,
    env: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> ResolvedProviderRoots {
    if !use_env {
        let root = home.join(".hermes");
        return ResolvedProviderRoots {
            paths: vec![root.clone()],
            default_root: Some(root),
            error: None,
        };
    }
    if let Some(value) = env("HERMES_HOME") {
        match value.into_string() {
            Ok(value) if !value.trim().is_empty() => {
                return vec![expand(home, value.trim(), |name| {
                    env(name).and_then(|v| v.into_string().ok())
                })]
                .into()
            }
            Err(value) => return vec![PathBuf::from(value)].into(),
            _ => {}
        }
    }
    let suffix = env("HERMES_DATA_DIR_SUFFIX")
        .and_then(|v| v.into_string().ok())
        .unwrap_or_default();
    let root = if windows {
        env("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join("AppData/Local"))
            .join(format!("hermes{suffix}"))
    } else {
        home.join(format!(".hermes{suffix}"))
    };
    ResolvedProviderRoots {
        paths: vec![root.clone()],
        default_root: Some(root),
        error: None,
    }
}

fn expand(home: &Path, value: &str, env: impl Fn(&str) -> Option<String>) -> PathBuf {
    let mut output = String::new();
    let mut chars = value.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            output.push(c);
            continue;
        }
        output.push_str(&variable(&mut chars, &env));
    }
    if output == "~" {
        home.into()
    } else if let Some(rest) = output.strip_prefix("~/") {
        home.join(rest)
    } else {
        output.into()
    }
}
fn variable(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
    env: &impl Fn(&str) -> Option<String>,
) -> String {
    let mut output = String::new();
    let braced = chars.peek() == Some(&'{');
    if braced {
        chars.next();
    }
    let mut name = String::new();
    while let Some(&c) = chars.peek() {
        if (braced && c == '}') || (!braced && !c.is_ascii_alphanumeric() && c != '_') {
            break;
        }
        name.push(c);
        chars.next();
    }
    let closed = !braced || chars.next() == Some('}');
    match env(&name).filter(|_| closed && !name.is_empty()) {
        Some(value) => output.push_str(&value),
        None => {
            output.push('$');
            if braced {
                output.push('{');
            }
            output.push_str(&name);
            if braced && closed {
                output.push('}');
            }
        }
    }
    output
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn home_and_environment_syntax_match_upstream_process_resolver() {
        let home = Path::new("/isolated/home");
        let env = |key: &str| (key == "ROOT").then(|| "/custom".into());
        assert_eq!(expand(home, "~/hermes", env), home.join("hermes"));
        assert_eq!(
            expand(home, "${ROOT}/profiles/coder", env),
            PathBuf::from("/custom/profiles/coder")
        );
        assert_eq!(
            expand(home, "$ROOT/state", env),
            PathBuf::from("/custom/state")
        );
        assert_eq!(
            expand(home, "$UNKNOWN/state", env),
            PathBuf::from("$UNKNOWN/state")
        );
        assert_eq!(resolve(home, false).paths, vec![home.join(".hermes")]);
    }
}
