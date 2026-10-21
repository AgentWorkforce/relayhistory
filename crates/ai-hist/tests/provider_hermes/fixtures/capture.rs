use ai_hist::{ProviderRoots, StoreOptions, SessionStore, DiscoveryOptions, Source, SessionRef, HydrateOptions};
use ai_hist::export::{ExportSnapshot, ExportSelection, ExportLimits, SessionIdentity};
#[test]
fn capture_pre_intake_index() {
 let out=std::path::PathBuf::from(std::env::var_os("HERMES_CAPTURE_OUT").unwrap());
 let home=out.join("home");
 std::fs::create_dir_all(home.join(".hermes")).unwrap();
 let native=rusqlite::Connection::open(home.join(".hermes/state.db")).unwrap();
 native.execute_batch(include_str!("fixtures/hermes/schema31.sql")).unwrap();drop(native);
 let path=out.join("legacy-index.db");
 let store=SessionStore::open(StoreOptions::default().db_path(&path).roots(ProviderRoots::from_home(home.clone(),home.join("opencode.db")))).unwrap();
 store.discover(DiscoveryOptions::default().sources([Source::Hermes])).unwrap();
 let sid="default:636861742f6e6f742d612d757569643aceb1";
 store.hydrate(&SessionRef::id(Source::Hermes,sid),HydrateOptions::default()).unwrap();
 let mut snapshot=ExportSnapshot::open(rusqlite::Connection::open(&path).unwrap(), &ExportSelection{sessions:vec![SessionIdentity{source:"hermes".into(),session_id:sid.into()}], kinds:vec!["session_event".into(),"tool_call".into(),"session_marker".into()],..Default::default()}, &ExportLimits{max_batch_records:1,..Default::default()},60000,1000).unwrap();
 let mut cursor=Some(snapshot.handle().cursor);let mut records=vec![];
 while let Some(c)=cursor {let page=snapshot.page(&c,1000).unwrap();cursor=page.next_cursor;records.extend(page.records);}
 let mut evidence:Vec<ai_hist::EvidenceRecord>=records.iter().map(|r| {let mut payload=r.payload.as_object().unwrap().clone();payload.remove("id");payload.remove("location");ai_hist::EvidenceRecord{kind:serde_json::from_value(serde_json::json!(r.kind)).unwrap(),payload,record_id:Some(r.record_id.clone()),revision_id:Some(r.revision_id.clone())}}).collect();
 let key=ai_hist::ObservationKey{source:"hermes".into(),session_id:sid.into(),location:ai_hist::SessionLocation::Remote,connector_id:"legacy".into(),connector_instance:"fixture".into()};
 let rejection=ai_hist::source_evidence::validate_records(&key,&[ai_hist::EvidenceKind::SessionEvent,ai_hist::EvidenceKind::ToolCall,ai_hist::EvidenceKind::SessionMarker],&mut evidence).unwrap_err().to_string();
 assert!(rejection.contains("tool_use_id is only valid on a tool_result event"),"{rejection}");
 std::fs::write(out.join("archive.json"),serde_json::to_string_pretty(&records).unwrap()+"\n").unwrap();
 std::fs::write(out.join("baseline-rejection.txt"),rejection+"\n").unwrap();
 drop(snapshot);drop(store);
}
