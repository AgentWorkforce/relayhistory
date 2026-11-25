// Pins the safety properties of the approval-gated fast path: it can only be
// dispatched by hand, starts with read-only permissions, and nothing runs
// until the fast-path environment (Khaliq's approval) passes.
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const workflow = readFileSync(new URL("../.github/workflows/fast-path.yml", import.meta.url), "utf8")
const lines = workflow.split("\n").filter(line => line.trim() !== "" && !line.trim().startsWith("#"))
const indent = line => line.length - line.trimStart().length

// Child lines of a block, given the index of its header line.
function block(start) {
    const base = indent(lines[start])
    const body = []
    for (let i = start + 1; i < lines.length && indent(lines[i]) > base; i++) body.push(lines[i])
    return body
}
// Direct keys of a block body: name -> inline value.
function keys(body) {
    if (body.length === 0) return new Map()
    const step = indent(body[0])
    const map = new Map()
    for (const line of body) {
        if (indent(line) !== step) continue
        const match = /^\s*([A-Za-z0-9_-]+):\s*(.*)$/.exec(line)
        if (match) map.set(match[1], match[2])
    }
    return map
}
const topIndex = name => lines.findIndex(line => line === `${name}:` || line.startsWith(`${name}: `))
const top = name => {
    const index = topIndex(name)
    assert.notEqual(index, -1, `missing top-level ${name}`)
    return block(index)
}

const jobsBody = top("jobs")
const jobStep = indent(jobsBody[0])
const jobs = new Map()
for (let i = 0; i < jobsBody.length; i++) {
    const match = indent(jobsBody[i]) === jobStep && /^\s*([A-Za-z0-9_-]+):\s*$/.exec(jobsBody[i])
    if (!match) continue
    const body = []
    for (let j = i + 1; j < jobsBody.length && indent(jobsBody[j]) > jobStep; j++) body.push(jobsBody[j])
    jobs.set(match[1], keys(body))
}
const needsOf = job => {
    const value = (jobs.get(job).get("needs") ?? "").trim()
    if (value === "") return []
    return value.startsWith("[") ? value.slice(1, -1).split(",").map(name => name.trim()).filter(Boolean) : [value]
}
const gate = [...jobs.keys()][0]

test("fast path is workflow_dispatch only", () => {
    assert.deepEqual([...keys(top("on")).keys()], ["workflow_dispatch"])
})

test("top-level permissions are contents: read only", () => {
    assert.deepEqual([...keys(top("permissions")).entries()], [["contents", "read"]])
})

test("only the first job uses the fast-path environment", () => {
    assert.equal(jobs.get(gate).get("environment"), "fast-path")
    for (const [name, job] of jobs) if (name !== gate) assert.equal(job.has("environment"), false, `${name} sets an environment`)
})

test("every other job needs the gate, directly or transitively", () => {
    const reaches = (job, seen = new Set()) => {
        if (job === gate) return true
        if (seen.has(job)) return false
        seen.add(job)
        return needsOf(job).some(need => {
            assert.ok(jobs.has(need), `${job} needs unknown job ${need}`)
            return reaches(need, seen)
        })
    }
    for (const name of jobs.keys()) if (name !== gate) assert.ok(reaches(name), `${name} can run before approval`)
})

test("concurrency never lets a dispatch cancel one waiting for approval", () => {
    const concurrency = keys(top("concurrency"))
    assert.match(concurrency.get("group") ?? "", /github\.run_id/)
    assert.equal(concurrency.get("cancel-in-progress"), "false")
})

test("the fast path never saves into a cache other workflows restore", () => {
    assert.doesNotMatch(workflow, /^\s+cache: ('|")?(npm|pnpm|yarn|true)('|")?\s*$/m)
    assert.doesNotMatch(workflow, /enable-cache: true|cache-to:|uses: actions\/cache@/)
    const rustCaches = workflow.match(/Swatinem\/rust-cache@/g)?.length ?? 0
    const saveOff = workflow.match(/save-if: false/g)?.length ?? 0
    assert.ok(saveOff >= rustCaches, "every rust-cache must set save-if: false")
})
