use goose::config::GooseMode;
use goose::conversation::{message::Message, Conversation};
use goose::session::{SessionManager, SessionType};
use rmcp::model::{CallToolRequestParams, CallToolResult, ContentBlock};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let phase = args.next().expect("phase");
    let root = PathBuf::from(args.next().expect("isolated data dir"));
    let manager = SessionManager::new(root.clone());
    let id_path = root.join("fixture-id.txt");
    if phase == "write" {
        let session = manager.create_session(root.join("workspace"), "Synthetic Goose history".into(), SessionType::User, GooseMode::Auto).await?;
        std::fs::write(&id_path, &session.id)?;
        manager.add_message(&session.id, &Message::user().with_id("user-native-1").with_text("Find the Goose fixture needle")).await?;
        manager.add_message(&session.id, &Message::assistant().with_id("assistant-native-1").with_text("I will inspect the fixture.").with_tool_request("call-native-1", Ok(CallToolRequestParams::new("read_file")))).await?;
        manager.add_message(&session.id, &Message::user().with_id("tool-native-1").with_tool_response("call-native-1", Ok(CallToolResult::success(vec![ContentBlock::text("The Goose fixture needle is here.")])))).await?;
        manager.add_message(&session.id, &Message::assistant().with_id("assistant-native-2").with_text("Found the Goose fixture needle.")).await?;
    } else if phase == "read" {
        let id = std::fs::read_to_string(&id_path)?;
        let session = manager.get_session(&id, true).await?;
        let exported = manager.export_session(&id).await?;
        assert_eq!(session.conversation.as_ref().unwrap().messages().len(), 4);
        std::fs::write(root.join("official-reader.json"), exported)?;
    } else if phase == "resume" {
        let id = std::fs::read_to_string(&id_path)?;
        manager.add_message(&id, &Message::user().with_id("user-native-2").with_text("Continue after restart.")).await?;
        manager.add_message(&id, &Message::assistant().with_id("assistant-native-3").with_text("The historical answer is still available.")).await?;
    } else if phase == "read-resumed" {
        let id = std::fs::read_to_string(&id_path)?;
        let session = manager.get_session(&id, true).await?;
        let exported = manager.export_session(&id).await?;
        assert_eq!(session.conversation.as_ref().unwrap().messages().len(), 6);
        std::fs::write(root.join("official-reader-resumed.json"), exported)?;
    } else if phase == "rewrite" {
        let id = std::fs::read_to_string(&id_path)?;
        let conversation = Conversation::new_unvalidated(vec![
            Message::user().with_id("user-native-1").with_text("Find the Goose fixture needle"),
            Message::assistant().with_id("assistant-native-2").with_text("The tool result was removed during rewrite."),
            Message::assistant().with_id("assistant-native-1").with_text("I will inspect the fixture.").with_tool_request("call-native-1", Ok(CallToolRequestParams::new("read_file"))),
            Message::user().with_id("user-native-2").with_text("Continue after restart."),
            Message::assistant().with_id("assistant-native-3").with_text("The revised historical answer is available."),
        ]);
        manager.replace_conversation(&id, &conversation).await?;
    } else if phase == "read-rewritten" {
        let id = std::fs::read_to_string(&id_path)?;
        let session = manager.get_session(&id, true).await?;
        let exported = manager.export_session(&id).await?;
        assert_eq!(session.conversation.as_ref().unwrap().messages().len(), 5);
        std::fs::write(root.join("official-reader-rewritten.json"), exported)?;
    } else {
        anyhow::bail!("unknown phase");
    }
    Ok(())
}
