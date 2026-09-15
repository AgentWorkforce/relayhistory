use super::{home, user_error};
use anyhow::{ensure, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    fs,
    io::Read,
    time::{Duration, Instant},
};
use url::Url;

pub fn site_origin(raw: &str) -> Result<String> {
    let url = Url::parse(raw)?;
    let local = is_local(&url);
    ensure!(
        (local && url.scheme() == "http")
            || (!local
                && url.scheme() == "https"
                && url.host_str() == Some("agentrelay.com")
                && url.port().is_none()),
        "untrusted site"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "site must be an origin"
    );
    Ok(url.origin().ascii_serialization())
}
fn is_local(url: &Url) -> bool {
    matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]"))
}
pub fn history_origin(raw: &str, site: &str) -> Result<()> {
    let url = Url::parse(raw)?;
    let local = is_local(&Url::parse(site)?);
    ensure!(
        (local && is_local(&url) && url.scheme() == "http")
            || (!local
                && url.scheme() == "https"
                && url.host_str() == Some("history.agentrelay.com")
                && url.port().is_none()),
        "untrusted history origin"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none(),
        "invalid history URL"
    );
    Ok(())
}
fn json_response(response: ureq::Response) -> Result<Value> {
    let mut bytes = Vec::new();
    response
        .into_reader()
        .take(1_048_577)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 1_048_576, "response too large");
    Ok(serde_json::from_slice(&bytes)?)
}
fn request(
    method: &str,
    url: &str,
    token: Option<&str>,
    body: Option<Value>,
) -> Result<(u16, Value)> {
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(15))
        .build();
    let mut request = agent.request(method, url);
    if let Some(token) = token {
        request = request.set("Authorization", &format!("Bearer {token}"));
    }
    let response = match if let Some(body) = body {
        request.send_json(body)
    } else {
        request.call()
    } {
        Ok(response) => response,
        Err(ureq::Error::Status(_, response)) => response,
        Err(_) => {
            return Err(user_error(
                "Could not reach Cloud. Check your connection and try again.",
            ))
        }
    };
    let status = response.status();
    Ok((status, json_response(response)?))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Stored {
    api_url: String,
    access_token: String,
    access_token_expires_at: String,
}
pub fn connect(site: &str, force: bool) -> Result<String> {
    let base = format!("{site}/cloud");
    // Reuse eligible official CLI auth without modifying it or forwarding a
    // credential belonging to another host. The probe saves only scoped rth auth.
    if !force {
        if let Ok(raw) = fs::read(home()?.join(".agentworkforce/relay/cloud-auth.json")) {
            if let Ok(auth) = serde_json::from_slice::<Stored>(&raw) {
                if auth.api_url.trim_end_matches('/') == base
                    && chrono::DateTime::parse_from_rfc3339(&auth.access_token_expires_at)
                        .is_ok_and(|t| t.timestamp_millis() > super::collector::now() + 60_000)
                    && whoami(site, &auth.access_token).is_ok()
                {
                    return Ok(auth.access_token);
                }
            }
        }
    }
    let (status, grant) = request(
        "POST",
        &format!("{base}/api/v1/auth/device/start"),
        None,
        Some(json!({"client_name":"Agent Relay Probe"})),
    )?;
    ensure!(matches!(status, 200 | 201), "device start rejected");
    let code = grant["device_code"]
        .as_str()
        .ok_or_else(|| user_error("Cloud did not start device sign-in."))?;
    let verification = Url::parse(
        grant["verification_uri_complete"]
            .as_str()
            .or_else(|| grant["verification_uri"].as_str())
            .ok_or_else(|| user_error("Cloud did not return a sign-in URL."))?,
    )?;
    ensure!(
        verification.origin().ascii_serialization() == site
            && verification.username().is_empty()
            && verification.password().is_none(),
        "wrong sign-in origin"
    );
    println!("Open this URL to authorize your computer:\n{verification}");
    if grant["verification_uri_complete"].is_null() {
        println!("Code: {}", grant["user_code"].as_str().unwrap_or(""));
    }
    let deadline = Instant::now()
        + Duration::from_secs(grant["expires_in"].as_u64().unwrap_or(600).clamp(1, 900));
    let mut interval = grant["interval"].as_u64().unwrap_or(5).clamp(1, 60);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_secs(interval));
        let result = request(
            "POST",
            &format!("{base}/api/v1/auth/device/token"),
            None,
            Some(
                json!({"grant_type":"urn:ietf:params:oauth:grant-type:device_code","device_code":code}),
            ),
        );
        let (status, token) = match result {
            Ok(value) => value,
            Err(_) => continue,
        };
        if status == 200 {
            ensure!(
                token["api_url"]
                    .as_str()
                    .is_none_or(|url| url.trim_end_matches('/') == base),
                "Cloud origin changed"
            );
            return token["access_token"]
                .as_str()
                .filter(|v| !v.is_empty())
                .map(String::from)
                .ok_or_else(|| user_error("Cloud sign-in did not return credentials."));
        }
        if status == 429 || token["error"] == "slow_down" {
            interval = (interval + 5).min(60);
            continue;
        }
        if status >= 500 || token["error"] == "authorization_pending" {
            continue;
        }
        return Err(user_error(
            "Device sign-in was declined or expired. Run setup again.",
        ));
    }
    Err(user_error("Device sign-in expired. Run setup again."))
}
#[derive(Deserialize)]
pub struct User {
    pub id: String,
}
#[derive(Deserialize)]
pub struct Workspace {
    pub id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub user: User,
    pub current_workspace: Option<Workspace>,
}
pub fn whoami(site: &str, token: &str) -> Result<Identity> {
    let (status, value) = request(
        "GET",
        &format!("{site}/cloud/api/v1/auth/whoami"),
        Some(token),
        None,
    )?;
    ensure!(status == 200, "Cloud authentication failed");
    Ok(serde_json::from_value(value)?)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Session {
    pub base_url: String,
    pub org_id: String,
    pub workspace_id: String,
    pub access_token: String,
    pub refresh_token: String,
    pub access_token_expires_at: String,
    pub scopes: Vec<String>,
}
pub fn history_session(site: &str, token: &str, workspace: &str) -> Result<Session> {
    let (status, value) = request(
        "POST",
        &format!("{site}/cloud/api/v1/workspaces/{workspace}/relayhistory/session"),
        Some(token),
        Some(json!({"mode":"sync","label":"Probe setup pending"})),
    )?;
    if status != 200 {
        return Err(user_error(
            "Cloud could not connect this workspace to RelayHistory.",
        ));
    }
    Ok(serde_json::from_value(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn origins_are_bound_to_the_selected_environment() {
        assert_eq!(
            site_origin("http://127.0.0.1:3100").unwrap(),
            "http://127.0.0.1:3100"
        );
        for url in [
            "https://evil.example",
            "http://agentrelay.com",
            "https://user:secret@agentrelay.com",
            "http://localhost:3100/path",
            "http://localhost:3100/?x=y",
        ] {
            assert!(site_origin(url).is_err());
        }
        assert!(history_origin("https://history.agentrelay.com", "http://127.0.0.1:3100").is_err());
        assert!(history_origin("http://127.0.0.1:3102", "https://agentrelay.com").is_err());
        assert!(history_origin("http://127.0.0.1:3102", "http://127.0.0.1:3100").is_ok());
    }
    fn device_server(status: u16, wrong_origin: bool) -> (String, std::thread::JoinHandle<()>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let site = format!("http://{}", listener.local_addr().unwrap());
        let origin = site.clone();
        let server = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            let count = if wrong_origin { 1 } else { 2 };
            for index in 0..count {
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(10))
                        }
                        Err(_) => panic!("device client did not complete expected request"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0; 1024];
                while !request.windows(4).any(|v| v == b"\r\n\r\n") {
                    let len = stream.read(&mut buffer).unwrap();
                    assert!(len > 0);
                    request.extend_from_slice(&buffer[..len]);
                }
                let headers = String::from_utf8_lossy(&request);
                assert!(headers.starts_with(if index == 0 {
                    "POST /cloud/api/v1/auth/device/start "
                } else {
                    "POST /cloud/api/v1/auth/device/token "
                }));
                assert!(!headers.to_lowercase().contains("authorization:"));
                let body = if index == 0 {
                    json!({"device_code":"synthetic-device-code", "verification_uri_complete":format!("{}/cloud/device?user_code=synthetic-code", if wrong_origin { "https://other.invalid" } else { &origin }), "expires_in":10, "interval":1})
                } else {
                    json!({"access_token":"synthetic-access-token", "api_url":format!("{origin}/cloud")})
                }.to_string();
                let response_status = if index == 0 { status } else { 200 };
                write!(stream, "HTTP/1.1 {response_status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        (site, server)
    }
    #[test]
    fn device_auth_accepts_cloud_created_response_and_polls_without_bearer() {
        let (site, server) = device_server(201, false);
        assert_eq!(connect(&site, true).unwrap(), "synthetic-access-token");
        server.join().unwrap();
    }
    #[test]
    fn device_auth_rejects_cross_origin_approval_link_before_polling() {
        let (site, server) = device_server(201, true);
        assert!(connect(&site, true).is_err());
        server.join().unwrap();
    }
}
