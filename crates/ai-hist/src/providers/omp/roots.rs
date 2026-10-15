use super::*;

pub(super) fn resolve(home: &Path, use_env: bool) -> ResolvedProviderRoots {
    let env = |key| use_env.then(|| std::env::var(key).ok()).flatten();
    let values = RootValues {
        session: env("PI_CODING_AGENT_SESSION_DIR"),
        agent: env("PI_CODING_AGENT_DIR"),
        config: env("PI_CONFIG_DIR"),
        profile: env("OMP_PROFILE").or_else(|| env("PI_PROFILE")),
        data: env("XDG_DATA_HOME"),
    };
    let profile = values
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty() && *v != "default");
    if profile.is_some_and(|p| !valid_profile(p)) {
        return ResolvedProviderRoots { error: Some("OMP_INVALID_PROFILE: use 1-64 lowercase safe filename characters, excluding reserved device names".into()), ..Default::default() };
    }
    from_values(home, &values).into()
}
#[derive(Default)]
pub(super) struct RootValues {
    pub session: Option<String>,
    pub agent: Option<String>,
    pub config: Option<String>,
    pub profile: Option<String>,
    pub data: Option<String>,
}
fn nonempty(value: &Option<String>) -> Option<&str> {
    value.as_deref().filter(|v| !v.is_empty())
}
fn expand(home: &Path, value: &str) -> PathBuf {
    if value == "~" {
        home.into()
    } else if let Some(rest) = value.strip_prefix("~/") {
        home.join(rest)
    } else {
        PathBuf::from(value)
    }
}
pub(super) fn from_values(home: &Path, v: &RootValues) -> Vec<PathBuf> {
    if let Some(session) = nonempty(&v.session) {
        return vec![expand(home, session)];
    }
    let profile = v
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty() && *v != "default");
    if let Some(profile) = profile {
        if !valid_profile(profile) {
            return Vec::new();
        }
    }
    let config = home.join(nonempty(&v.config).unwrap_or(".omp"));
    let config = profile.map_or_else(|| config.clone(), |p| config.join("profiles").join(p));
    let default_agent = config.join("agent");
    let agent = if profile.is_some() {
        default_agent.clone()
    } else {
        nonempty(&v.agent)
            .map(|s| expand(home, s))
            .unwrap_or_else(|| default_agent.clone())
    };
    if cfg!(any(target_os = "macos", target_os = "linux")) && agent == default_agent {
        if let Some(data) = nonempty(&v.data) {
            let data = Path::new(data).join("omp");
            let data = profile.map_or_else(|| data.clone(), |p| data.join("profiles").join(p));
            if data.exists() {
                return vec![data.join("sessions")];
            }
        }
    }
    vec![agent.join("sessions")]
}
fn valid_profile(p: &str) -> bool {
    let stem = p.split('.').next().unwrap_or_default();
    let device = ["con", "prn", "aux", "nul"].contains(&stem)
        || (stem.len() == 4
            && (stem.starts_with("com") || stem.starts_with("lpt"))
            && stem.as_bytes()[3].is_ascii_digit());
    p.len() <= 64
        && p.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && p.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
        && !p.ends_with('.')
        && !device
}
