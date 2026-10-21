import java.nio.file.*;
import java.util.*;
import com.intellij.ml.llm.matterhorn.ej.app.cli.standalone.tui.app.state.*;
import org.jetbrains.a2ux.extensions.*;
import org.jetbrains.a2ux.api.*;
public class WriteCapture {
 static final String SID="session-261021-120000-demo";
 static final Path HOME=Path.of(System.getProperty("synthetic.home"));
 static final SessionStore STORE=SessionStore.INSTANCE;
 static void event(AgentEvent event, A2uxTaskState state) {
  STORE.storeEvent(HOME,new SessionA2uxEvent(new A2uxEvent(state,event),ExecutionMode.Standard,"task-native-1",null),SID);
 }
 static void prompt(String req, String text) {
  STORE.storeEvent(HOME,new UserPromptEvent(req,null,text,null,List.of(),List.of(),List.of(),null,true,null,UserPromptDelivery.Sent,MessageChannel.Cli,null,null,null,null,null,null,null,null,null,null),SID);
 }
 public static void main(String[] args) {
  if(args.length != 1) throw new IllegalArgumentException("phase: initial or resume");
  if(args[0].equals("initial")) {
   STORE.storeSessionSummary(HOME,SID,new SessionSummary(SID,1792609200000L,1792609201000L,"/synthetic/project","Synthetic Junie capture","running"));
   prompt("req-1","Hello from synthetic Junie");
   event(new MarkdownBlockUpdatedEvent(null,"step-md-1",null,"I will inspect a synthetic file."),A2uxTaskState.IN_PROGRESS);
   event(new ToolBlockUpdatedEvent(null,"step-tool-1",null,null,null,"Read synthetic.txt",null,StepStatus.IN_PROGRESS,null,null,null,null,null),A2uxTaskState.IN_PROGRESS);
   event(new ToolBlockUpdatedEvent(null,"step-tool-1",null,null,null,"Read synthetic.txt",null,StepStatus.COMPLETED,"path: synthetic.txt",null,"blue",null,null),A2uxTaskState.IN_PROGRESS);
   event(new ResultBlockUpdatedEvent(null,"step-result-1",null,false,"The synthetic file is blue.",null,List.of(),null,null,null,null,null),A2uxTaskState.COMPLETED);
  } else if(args[0].equals("resume")) {
   STORE.storeSessionSummary(HOME,SID,new SessionSummary(SID,1792609200000L,1792609205000L,"/synthetic/project","Synthetic Junie capture","completed"));
   prompt("req-2","Continue after restart");
   event(new SubagentSpawnedEvent(null,"step-child-1",null,"Researcher","research","Check a synthetic color","task-native-1"),A2uxTaskState.IN_PROGRESS);
   event(new ResultBlockUpdatedEvent(null,"step-result-2",null,false,"The child confirmed blue.",null,List.of(),null,null,null,null,null),A2uxTaskState.COMPLETED);
  } else throw new IllegalArgumentException(args[0]);
  System.out.println(STORE.readStoredEventsFromFile(HOME,SID).size());
 }
}
