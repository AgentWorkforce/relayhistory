"""Synthetic native DeepAgents Code checkpoint capture, pinned in README."""
import asyncio
import json
import sqlite3
from pathlib import Path
from typing import Annotated, TypedDict
import aiosqlite
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, RemoveMessage
from deepagents._messages_reducer import _messages_delta_reducer
from langgraph.channels.delta import DeltaChannel
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.graph import START, StateGraph
from langgraph.types import Overwrite

DB = Path(__file__).with_name('sessions.db')
THREAD = '019a0000-0000-7000-8000-000000000443'

async def main():
    DB.unlink(missing_ok=True)
    conn = await aiosqlite.connect(DB)
    saver = AsyncSqliteSaver(conn)
    config = {'configurable': {'thread_id': THREAD, 'checkpoint_ns': ''}}
    messages = [HumanMessage(content='Find the DeepAgents\nfixture needle', id='human-1'), AIMessage(content='Looking now', id='ai-1', tool_calls=[{'name':'search','args':{'query':'needle'},'id':'call-1'}]), ToolMessage(content='Found needle', id='tool-1', tool_call_id='call-1'), AIMessage(content='Found the DeepAgents fixture needle.', id='ai-2')]
    cp = empty_checkpoint()
    cp['id'] = '00000000-0000-7000-8000-000000000001'
    cp['channel_values'] = {'messages': messages}
    config = await saver.aput(config, cp, {'source':'loop','step':1,'updated_at':'2026-10-22T12:00:00+00:00','cwd':'/synthetic/project'}, {})
    cp2 = empty_checkpoint()
    cp2['id'] = '00000000-0000-7000-8000-000000000002'
    cp2['channel_values'] = {'messages': messages}
    config = await saver.aput(config, cp2, {'source':'loop','step':2,'updated_at':'2026-10-22T12:01:00+00:00','cwd':'/synthetic/project'}, {})
    await saver.aput_writes(config, [('messages',[HumanMessage(content='Continue after restart', id='human-2'),AIMessage(content='History survived restart.',id='ai-3')])], 'task-2')
    # The DeltaChannel form has no inline messages; ordered writes are its source.
    revised = {'configurable': {'thread_id': '019a0000-0000-7000-8000-000000000444', 'checkpoint_ns': ''}}
    cp3 = empty_checkpoint()
    cp3['id'] = '00000000-0000-7000-8000-000000000003'
    revised = await saver.aput(revised, cp3, {'source':'loop','step':1,'updated_at':'2026-10-22T13:00:00+00:00'}, {})
    await saver.aput_writes(revised, [('messages',[HumanMessage(content='Retained prompt',id='keep'),AIMessage(content='Old draft',id='revise'),AIMessage(content='Remove me',id='remove')])], 'task-a')
    cp4 = empty_checkpoint()
    cp4['id'] = '00000000-0000-7000-8000-000000000004'
    revised = await saver.aput(revised, cp4, {'source':'loop','step':2,'updated_at':'2026-10-22T13:01:00+00:00'}, {})
    await saver.aput_writes(revised, [('messages',[AIMessage(content='Revised answer',id='revise'),RemoveMessage(id='remove')])], 'task-b')
    cp5 = empty_checkpoint()
    cp5['id'] = '00000000-0000-7000-8000-000000000005'
    revised = await saver.aput(revised, cp5, {'source':'loop','step':3,'updated_at':'2026-10-22T13:02:00+00:00'}, {})
    await saver.aput_writes(revised, [('messages',Overwrite([HumanMessage(content='Compaction summary prompt',id='compact-human'),AIMessage(content='Compaction summary answer',id='compact-ai')]))], 'task-c')
    replacement = {'configurable': {'thread_id': '019a0000-0000-7000-8000-000000000445', 'checkpoint_ns': ''}}
    cp6 = empty_checkpoint()
    cp6['id'] = '00000000-0000-7000-8000-000000000006'
    replacement = await saver.aput(replacement, cp6, {'source':'loop','step':1,'updated_at':'2026-10-22T14:00:00+00:00'}, {})
    await saver.aput_writes(replacement, [('messages',[HumanMessage(content='Retained prompt',id='keep'),AIMessage(content='Old draft',id='revise'),AIMessage(content='Remove me',id='remove')])], 'task-a')
    cp7 = empty_checkpoint()
    cp7['id'] = '00000000-0000-7000-8000-000000000007'
    replacement = await saver.aput(replacement, cp7, {'source':'loop','step':2,'updated_at':'2026-10-22T14:01:00+00:00'}, {})
    await saver.aput_writes(replacement, [('messages',[AIMessage(content='Revised answer',id='revise'),RemoveMessage(id='remove')])], 'task-b')

    # Run the actual pinned SDK DeltaChannel reducer through a compiled graph,
    # with a deterministic node instead of an authenticated/model call.
    class State(TypedDict):
        messages: Annotated[list, DeltaChannel(_messages_delta_reducer, snapshot_frequency=50)]
    graph = StateGraph(State)
    graph.add_node('reply', lambda state: {'messages':[AIMessage(content=f'Actual DeltaChannel reply {len(state["messages"])}', id=f'graph-ai-{len(state["messages"])}')]})
    graph.add_edge(START, 'reply')
    compiled = graph.compile(checkpointer=saver)
    graph_config = {'configurable': {'thread_id': '019a0000-0000-7000-8000-000000000446'}}
    await compiled.ainvoke({'messages':[HumanMessage(content='Actual DeltaChannel prompt 0',id='graph-human-0')]}, graph_config)
    for index in range(1, 56):
        await compiled.ainvoke({'messages':[HumanMessage(content=f'Actual DeltaChannel prompt {index}',id=f'graph-human-{index}')]}, graph_config)
    # The post-snapshot write revises one stable native ID and removes another.
    await compiled.ainvoke({'messages':[AIMessage(content='Revised stable answer',id='graph-ai-1'),RemoveMessage(id='graph-human-2')]}, graph_config)
    state = await compiled.aget_state(graph_config)
    native = [{'id': m.id, 'role': m.type, 'content': m.content} for m in state.values['messages']]
    DB.with_name('native-446-state.json').write_text(json.dumps({'messages':native},indent=2)+'\n')
    await conn.close()
    with sqlite3.connect(DB) as db:
        rows = db.execute('SELECT checkpoint_id,type,length(checkpoint),metadata FROM checkpoints').fetchall()
        writes = db.execute('SELECT checkpoint_id,channel,type,length(value) FROM writes').fetchall()
        print(json.dumps({"checkpoints": [list(r[:3])+[r[3].decode()] for r in rows], "writes":writes},indent=2))

asyncio.run(main())
