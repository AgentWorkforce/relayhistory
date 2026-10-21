import java.nio.file.*;
import com.intellij.ml.llm.matterhorn.ej.app.cli.standalone.tui.app.state.*;
import org.jetbrains.a2ux.api.*;
public class ReadOracle {
 public static void main(String[] args) {
  for (StoredSessionEvent stored : SessionStore.INSTANCE.readStoredEventsFromFile(Path.of(args[0]),args[1])) {
   SessionEvent event=stored.getEvent();
   long ts=stored.getTimestampMs()==null?-1:stored.getTimestampMs();
   if(event instanceof UserPromptEvent e) System.out.println("user\t"+e.getRequestId()+"\t"+ts+"\t"+e.getPrompt());
   else if(event instanceof SessionA2uxEvent e) {
    AgentEvent a=e.getEvent().getAgentEvent();
    if(a instanceof MarkdownBlockUpdatedEvent m) System.out.println("assistant-markdown\t"+m.getStepId()+"\t"+ts+"\t"+m.getText());
    else if(a instanceof ResultBlockUpdatedEvent m) System.out.println("assistant-result\t"+m.getStepId()+"\t"+ts+"\t"+m.getResult());
    else if(a instanceof ToolBlockUpdatedEvent m) System.out.println("tool-"+m.getStatus()+"\t"+m.getStepId()+"\t"+ts+"\t"+m.getText()+"|"+m.getOutput());
    else if(a instanceof SubagentSpawnedEvent m) System.out.println("subagent\t"+m.getStepId()+"\t"+ts+"\t"+m.getName()+"|"+m.getParentId());
   }
  }
 }
}
