use relayhistory_plugin::delivery::{HistoryExportBatch, PreparedPayload};
use relayhistory_plugin::destination::{account_id, prepare, MAPPING_VERSION};
use serde_json::{json, Value};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
fn batch() -> HistoryExportBatch {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/delivery-native-v1.json")).unwrap();
    assert_eq!(
        fixture["batch"]["account_id"],
        account_id("org-fixture", Some("workspace-fixture"))
    );
    let mut batch: HistoryExportBatch = serde_json::from_value(fixture["batch"].clone()).unwrap();
    assert_eq!(batch.mapping_version, MAPPING_VERSION);
    batch.account_id = account_id("org-fixture", None);
    batch
}
fn invoke(home: &Path, operation: &str, args: Value) -> Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_relayhistory-plugin"))
        .env("HOME", home)
        .env("RELAYHISTORY_HOME", home)
        .env_remove("RELAYHISTORY_BASE_URL")
        .env_remove("AI_HIST_BASE_URL")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            json!({"version":1,"operation":operation,"args":args})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    serde_json::from_slice(&output.stdout).unwrap()
}
fn auth(home: &Path, base: &str, org: &str) {
    let stages = home.join("stages");
    std::fs::create_dir_all(&stages).unwrap();
    std::fs::write(
        stages.join(format!("{}.auth.json", ai_hist::prompt_hash(base))),
        json!({"base_url":base,"access_token":"rth_at_fixture","org_id":org}).to_string(),
    )
    .unwrap();
}
fn server(pages: Vec<(u16, Value)>) -> (String, thread::JoinHandle<Vec<(String, String)>>) {
    server_with_hook(pages, |_, _| {}, |_| {})
}
fn server_with_hook(
    pages: Vec<(u16, Value)>,
    hook: impl Fn(usize, &mut std::net::TcpStream) + Send + 'static,
    finished: impl FnOnce(&TcpListener) + Send + 'static,
) -> (String, thread::JoinHandle<Vec<(String, String)>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let handle = thread::spawn(move || {
        let mut requests = Vec::new();
        for (index, (status, body)) in pages.into_iter().enumerate() {
            let began = Instant::now();
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(began.elapsed() < Duration::from_secs(10));
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("{error}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut headers = String::new();
            let mut length = 0;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse().unwrap();
                }
                headers.push_str(&line);
            }
            let mut bytes = vec![0; length];
            reader.read_exact(&mut bytes).unwrap();
            requests.push((headers, String::from_utf8(bytes).unwrap()));
            hook(index, &mut stream);
            if status == 0 {
                continue;
            }
            let body = body.to_string();
            write!(stream,"HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
        }
        finished(&listener);
        requests
    });
    (base, handle)
}
/// Delivery arguments now carry the batch itself: the helper reruns the
/// destination's consent, account and instance guards before every dispatch.
/// A test host cannot inspect legacy schedules, so it acknowledges that
/// explicitly exactly as an interactive user would.
fn send_args(base: String, prepared: &PreparedPayload, batch: &HistoryExportBatch) -> Value {
    json!({"baseUrl":base,"prepared":prepared,"batch":batch,"acknowledgeUninspectedSchedules":true})
}
fn accepted(batch: &HistoryExportBatch) -> Value {
    json!({"protocolVersion":1,"batchId":batch.batch_id,"acceptedRevisionIds":batch.records.iter().map(|r|&r.revision_id).collect::<Vec<_>>(),"unsupportedRevisionIds":[],"acceptanceLevel":"durable"})
}
#[test]
fn native_fixture_prepares_immutably_and_retry_sends_identical_bytes() {
    let batch = batch();
    let prepared = prepare(batch.clone()).unwrap();
    let saved: PreparedPayload =
        serde_json::from_str(&serde_json::to_string(&prepared).unwrap()).unwrap();
    let (base, server) = server(vec![
        (0, json!({"secret":"rth_at_never_echo"})),
        (200, accepted(&batch)),
    ]);
    let home = tempfile::tempdir().unwrap();
    auth(home.path(), &base, "org-fixture");
    let first = invoke(
        home.path(),
        "deliverySend",
        send_args(base.clone(), &prepared, &batch),
    );
    assert_eq!(first["error"]["code"], "DELIVERY_TRANSIENT");
    assert!(!first.to_string().contains("rth_at_never_echo"));
    let retry = invoke(
        home.path(),
        "deliverySend",
        send_args(base.clone(), &saved, &batch),
    );
    assert_eq!(retry["ok"], true);
    assert_eq!(
        retry["value"]["accepted_revision_ids"]
            .as_array()
            .unwrap()
            .len(),
        batch.records.len()
    );
    let requests = server.join().unwrap();
    assert_eq!(requests[0].1, prepared.body);
    assert_eq!(requests[1].1, prepared.body);
    assert!(requests[0].0.starts_with("POST /v1/delivery/batches "));
}
#[test]
fn old_server_and_wrong_receipts_never_fall_back_or_acknowledge() {
    let batch = batch();
    let prepared = prepare(batch.clone()).unwrap();
    let mut wrong = accepted(&batch);
    wrong["acceptedRevisionIds"] = json!(["unsubmitted"]);
    for (status, body, code) in [
        (404, json!({}), "DELIVERY_UNSUPPORTED_EVIDENCE"),
        (200, json!({"accepted":99}), "DELIVERY_INVALID_PAYLOAD"),
        (200, wrong, "DELIVERY_INVALID_PAYLOAD"),
    ] {
        let (base, server) = server(vec![(status, body)]);
        let home = tempfile::tempdir().unwrap();
        auth(home.path(), &base, "org-fixture");
        let result = invoke(
            home.path(),
            "deliverySend",
            send_args(base.clone(), &prepared, &batch),
        );
        assert_eq!(result["error"]["code"], code);
        assert_eq!(server.join().unwrap().len(), 1);
    }
}
#[test]
fn account_mismatch_and_prepared_mutation_fail_before_transport() {
    let batch = batch();
    let mut prepared = prepare(batch.clone()).unwrap();
    let home = tempfile::tempdir().unwrap();
    let base = "http://127.0.0.1:1".to_string();
    auth(home.path(), &base, "other-org");
    let mismatch = invoke(
        home.path(),
        "deliverySend",
        send_args(base.clone(), &prepared, &batch),
    );
    assert_eq!(mismatch["error"]["code"], "DELIVERY_PERMISSION_DENIED");
    prepared.body.push(' ');
    let altered = invoke(
        home.path(),
        "deliverySend",
        send_args(base, &prepared, &batch),
    );
    assert_eq!(altered["error"]["code"], "DELIVERY_INVALID_PAYLOAD");
}

/// The receiver's own assertions about the generation a host configured are
/// rechecked on every prepare and send, not only when a job is created.
#[test]
fn asserted_account_and_instance_are_rechecked_before_every_dispatch() {
    let batch = batch();
    let prepared = prepare(batch.clone()).unwrap();
    let home = tempfile::tempdir().unwrap();
    let base = "http://127.0.0.1:1".to_string();
    auth(home.path(), &base, "org-fixture");
    for (extra, code) in [
        (
            json!({"expectedAccount": account_id("other-org", None)}),
            "DELIVERY_PERMISSION_DENIED",
        ),
        (
            json!({"instanceId": "another-generation"}),
            "DELIVERY_MAPPING_VERSION_MISMATCH",
        ),
    ] {
        for operation in ["deliveryPrepare", "deliverySend"] {
            let mut args = send_args(base.clone(), &prepared, &batch);
            for (key, value) in extra.as_object().unwrap() {
                args[key] = value.clone();
            }
            assert_eq!(invoke(home.path(), operation, args)["error"]["code"], code);
        }
    }
    // The same arguments matching the batch reach the transport instead.
    let mut args = send_args(base, &prepared, &batch);
    args["expectedAccount"] = json!(batch.account_id);
    args["instanceId"] = json!(batch.instance_id);
    assert_eq!(
        invoke(home.path(), "deliveryPrepare", args.clone())["value"]["sha256"],
        json!(prepared.sha256)
    );
    assert_eq!(
        invoke(home.path(), "deliverySend", args)["error"]["code"],
        "DELIVERY_TRANSIENT"
    );
}
#[test]
fn receipt_overlap_and_duplicate_revisions_are_rejected() {
    let batch = batch();
    let mut duplicate = batch.clone();
    duplicate.records.push(duplicate.records[0].clone());
    assert!(prepare(duplicate).is_err());
    let mut response = accepted(&batch);
    response["unsupportedRevisionIds"] = json!([batch.records[0].revision_id]);
    let (base, server) = server(vec![(200, response)]);
    let home = tempfile::tempdir().unwrap();
    auth(home.path(), &base, "org-fixture");
    let result = invoke(
        home.path(),
        "deliverySend",
        send_args(base.clone(), &prepare(batch.clone()).unwrap(), &batch),
    );
    assert_eq!(result["error"]["code"], "DELIVERY_INVALID_PAYLOAD");
    server.join().unwrap();
}
#[test]
fn readback_pins_account_and_explicitly_reports_live_pagination() {
    let record = batch().records.remove(0);
    let (base, server) = server(vec![(
        200,
        json!({"protocolVersion":1,"listing":"live","records":[record],"nextCursor":"next"}),
    )]);
    let home = tempfile::tempdir().unwrap();
    auth(home.path(), &base, "org-fixture");
    let expected = account_id("org-fixture", None);
    let result = invoke(
        home.path(),
        "deliveryRead",
        json!({"baseUrl":base,"readOptions":{"expectedAccount":expected,"limit":1}}),
    );
    assert_eq!(result["ok"], true);
    assert_eq!(result["value"]["listing"], "live");
    let requests = server.join().unwrap();
    assert!(requests[0]
        .0
        .contains(&format!("X-RelayHistory-Expected-Account: {expected}")));
    assert!(requests[0]
        .0
        .starts_with("GET /v1/delivery/records?limit=1 "));
}
#[test]
fn default_size_payload_survives_helper_json_escaping() {
    let mut batch = batch();
    batch.records.truncate(1);
    batch.records[0].payload = json!({"prompt":"\u{1}".repeat(150_000)});
    let home = tempfile::tempdir().unwrap();
    let result = invoke(
        home.path(),
        "deliveryPrepare",
        json!({"batch":batch,"acknowledgeUninspectedSchedules":true}),
    );
    assert_eq!(result["ok"], true);
    let prepared: PreparedPayload = serde_json::from_value(result["value"].clone()).unwrap();
    assert!(prepared.body.len() > 900_000);
    assert!(prepared.body.len() < 1_048_576);
}

#[test]
fn preparation_matches_server_record_limit_before_persisting_invalid_payload() {
    let mut batch = batch();
    let template = batch.records[0].clone();
    batch.records = (0..100)
        .map(|index| {
            let mut record = template.clone();
            record.record_id = format!("record-{index}");
            record.revision_id = format!("revision-{index}");
            record
        })
        .collect();
    assert!(prepare(batch.clone()).is_ok());
    let mut extra = template;
    extra.record_id = "record-100".into();
    extra.revision_id = "revision-100".into();
    batch.records.push(extra);
    assert!(prepare(batch).is_err());
}

#[test]
fn receiver_deadlines_and_cancellation_cover_transport_refresh_and_lock_waits() {
    // Isolate stored credentials and schedule discovery from other tests and
    // from the host. All HTTP traffic goes to synthetic loopback receivers.
    const CHILD: &str = "RELAY_TEST_RECEIVER_DEADLINE";
    if std::env::var_os(CHILD).is_none() {
        let home = tempfile::tempdir().unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "receiver_deadlines_and_cancellation_cover_transport_refresh_and_lock_waits",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("HOME", home.path())
            .env("RELAYHISTORY_HOME", home.path())
            .env_remove("RELAYHISTORY_BASE_URL")
            .env_remove("AI_HIST_BASE_URL")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use relayhistory_plugin::delivery::{
        worker::{Receiver, ReceiverContext},
        DeliveryFailure,
    };
    use relayhistory_plugin::destination::RelayHistoryReceiver;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    };
    let home = std::env::var_os("RELAYHISTORY_HOME").unwrap();
    let home = Path::new(&home);
    let batch = batch();
    let prepared = prepare(batch.clone()).unwrap();
    let cancelled = ReceiverContext {
        cancelled: &|| true,
        timeout_ms: 60_000,
        idempotency_key: &batch.batch_id,
    };
    let receiver = RelayHistoryReceiver::default();
    assert_eq!(
        receiver.prepare(&batch, &cancelled).unwrap_err().failure,
        DeliveryFailure::Transient
    );
    assert_eq!(
        receiver
            .send(&prepared, &batch, &cancelled)
            .unwrap_err()
            .failure,
        DeliveryFailure::Transient
    );

    for phase in [
        "headers",
        "body",
        "refresh",
        "lock",
        "cancel-refresh",
        "cancel-retry",
        "success",
    ] {
        let cancelled = Arc::new(AtomicBool::new(false));
        let server_cancelled = cancelled.clone();
        let (entered, ready) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let pages = match phase {
            "refresh" | "cancel-retry" => vec![
                (401, json!({})),
                (
                    if phase == "refresh" { 0 } else { 200 },
                    json!({"accessToken":"rth_at_rotated", "refreshToken":"rth_rt_rotated"}),
                ),
            ],
            "lock" | "cancel-refresh" => vec![(401, json!({}))],
            "success" => vec![(200, accepted(&batch))],
            _ => vec![(0, json!({}))],
        };
        let (completed, completion) = mpsc::channel();
        let (base, server) = server_with_hook(
            pages,
            move |index, stream| {
                if phase == "cancel-refresh" || (phase == "cancel-retry" && index == 1) {
                    server_cancelled.store(true, Ordering::SeqCst);
                }
                if phase == "body" {
                    write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n").unwrap();
                }
                if matches!(phase, "headers" | "body") || (phase == "refresh" && index == 1) {
                    entered.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(10)).unwrap();
                }
                if phase == "success" {
                    thread::sleep(Duration::from_millis(150));
                }
            },
            move |listener| {
                completion.recv_timeout(Duration::from_secs(10)).unwrap();
                // Keep listening until the caller has returned: a forbidden refresh
                // or retry would otherwise just get connection-refused and look transient.
                assert!(
                    matches!(listener.accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
                    "{phase}: unexpected additional request"
                );
            },
        );
        auth(home, &base, "org-fixture");
        let auth_path = home
            .join("stages")
            .join(format!("{}.auth.json", ai_hist::prompt_hash(&base)));
        let mut stored: Value =
            serde_json::from_slice(&std::fs::read(&auth_path).unwrap()).unwrap();
        stored["refresh_token"] = json!("rth_rt_fixture");
        std::fs::write(&auth_path, stored.to_string()).unwrap();
        let lock = if phase == "lock" {
            let path = auth_path.with_file_name(format!(
                "{}.refresh.lock",
                auth_path.file_name().unwrap().to_str().unwrap()
            ));
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)
                .unwrap();
            fs2::FileExt::lock_exclusive(&file).unwrap();
            Some(file)
        } else {
            None
        };
        let receiver = RelayHistoryReceiver {
            base_url: Some(base),
            acknowledge_uninspected_schedules: true,
            ..Default::default()
        };
        let payload = prepared.clone();
        let batch = batch.clone();
        let (done, result) = mpsc::channel();
        let worker = thread::spawn(move || {
            let context = ReceiverContext {
                cancelled: &|| cancelled.load(Ordering::SeqCst),
                timeout_ms: if phase == "success" { 60_000 } else { 500 },
                idempotency_key: &batch.batch_id,
            };
            done.send(receiver.send(&payload, &batch, &context))
                .unwrap();
        });
        if matches!(phase, "headers" | "body" | "refresh") {
            ready.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        let outcome = result.recv_timeout(Duration::from_secs(2));
        // Always unblock fixtures before asserting so a regression cannot hang.
        let _ = release.send(());
        drop(lock);
        worker.join().unwrap();
        completed.send(()).unwrap();
        let requests = server.join().unwrap();
        let outcome =
            outcome.unwrap_or_else(|_| panic!("{phase}: receiver exceeded the supplied budget"));
        if phase == "success" {
            assert!(outcome.is_ok());
        } else {
            assert_eq!(
                outcome.unwrap_err().failure,
                DeliveryFailure::Transient,
                "{phase}"
            );
        }
        assert_eq!(
            requests.len(),
            if matches!(phase, "refresh" | "cancel-retry") {
                2
            } else {
                1
            }
        );
        if phase == "cancel-retry" {
            let rotated: Value =
                serde_json::from_slice(&std::fs::read(&auth_path).unwrap()).unwrap();
            assert_eq!(rotated["refresh_token"], "rth_rt_rotated");
        }
    }
}
