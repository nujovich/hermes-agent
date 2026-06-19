# Hermes Recon Report — `prompt-size` & `sessions optimize`

**Repo:** `nujovich/hermes-agent` (fork of `NousResearch/hermes-agent`)
**Branch:** `claude/hermes-prompt-size-recon-amx6ib`
**Scope:** Read-only reconnaissance — no code changed.

---

## 1. `hermes prompt-size` — Implementation

### Dispatch chain

```
hermes_cli/main.py:12321        build_prompt_size_parser(subparsers, cmd_prompt_size=cmd_prompt_size)
hermes_cli/main.py:10886        def cmd_prompt_size(args) — thin lazy wrapper
hermes_cli/prompt_size.py:141   def cmd_prompt_size(args)  — entry point
hermes_cli/prompt_size.py:52    def compute_prompt_breakdown(platform) — core logic
```

Parser: `hermes_cli/subcommands/prompt_size.py:12`. Flags: `--platform` (default `cli`), `--json`.

### Block assembly (`hermes_cli/prompt_size.py`)

| Block | Source |
|---|---|
| Offline agent | `_build_inspection_agent(platform)` — `AIAgent(api_key="inspect-only", ...)`, no network call |
| Prompt tiers (stable/context/volatile) | `build_system_prompt_parts(agent)` from `agent.system_prompt` |
| Skills index | `_SKILLS_BLOCK_RE.search(stable)` — regex on `<available_skills>…</available_skills>` (`prompt_size.py:21`) |
| Memory + user profile | `agent._memory_store.format_for_system_prompt("memory"/"user")` (`prompt_size.py:80–88`) |
| Tool schemas | `json.dumps(getattr(agent, "tools", None) or [])` (`prompt_size.py:91–92`) |

### Tokenizer / estimator used

**None.** The module only uses `len(s.encode("utf-8"))` (`_bytes`, `prompt_size.py:24`) and `len(s)` for chars. All output is **bytes/chars only** — no token estimation is performed.

> **Key surprise:** Despite the command name and bug #23767 mentioning "token estimation," `hermes_cli/prompt_size.py` never calls any token estimator. The `(len(text)+3)//4` heuristic referenced in #23767 lives in `agent/model_metadata.py:1897` (`estimate_tokens_rough`) — used by the conversation loop and compressor, not by `prompt-size`.

### Reusability

`compute_prompt_breakdown()` is a clean, importable free function. Already reused by the web server at `hermes_cli/web_server.py:1927` (`@app.post("/api/ops/prompt-size")`). Not CLI-bound.

---

## 2. `hermes sessions optimize` — Existence Audit

**Status: Fully implemented and wired.**

| Layer | Location |
|---|---|
| Parser registration | `hermes_cli/main.py:11949` |
| Dispatch handler | `hermes_cli/main.py:12192` — `elif action == "optimize":` in `cmd_sessions` |
| DB method | `db.vacuum()` → `optimize_fts()` + SQLite `VACUUM` |
| `optimize_fts()` | `hermes_state.py:4529` — merges FTS5 b-tree segments |
| `vacuum()` | `hermes_state.py:4582` — calls `optimize_fts()` then SQLite `VACUUM` |

### What it does

Reports DB size before/after. Merges FTS5 segments for `messages_fts` and `messages_fts_trigram`, then runs SQLite `VACUUM`. No data change — pure maintenance.

### Test / doc status

- DB layer tested in `tests/test_hermes_state.py:TestOptimizeFts` (lines 3268–3321) — 4 tests.
- **No integration test** for the CLI dispatch path.
- **Not documented** in the `hermes sessions` subcommand list — upstream PR #34670 addresses this.

---

## 3. `_HERMES_CORE_TOOLS` and Tool Search Bridge

### Core tools (`toolsets.py:31–76`) — 44 tools, always in schema

| Category | Tools |
|---|---|
| Web | `web_search`, `web_extract` |
| Terminal | `terminal`, `process`, `read_terminal` |
| File | `read_file`, `write_file`, `patch`, `search_files` |
| Vision | `vision_analyze`, `image_generate` |
| Skills | `skills_list`, `skill_view`, `skill_manage` |
| Browser | `browser_navigate`, `browser_snapshot`, `browser_click`, `browser_type`, `browser_scroll`, `browser_back`, `browser_press`, `browser_get_images`, `browser_vision`, `browser_console`, `browser_cdp`, `browser_dialog` |
| Misc | `text_to_speech`, `todo`, `memory`, `session_search`, `clarify`, `execute_code`, `delegate_task`, `cronjob`, `send_message` |
| Home Assistant | `ha_list_entities`, `ha_get_state`, `ha_list_services`, `ha_call_service` |
| Kanban | `kanban_show`, `kanban_list`, `kanban_complete`, `kanban_block`, `kanban_heartbeat`, `kanban_comment`, `kanban_create`, `kanban_link`, `kanban_unblock` |
| Computer use | `computer_use` |

### Tool Search bridge (`tools/tool_search.py`)

Three bridge tools replace all non-core tools when Tool Search is active:
- `tool_search` — `tools/tool_search.py:43`
- `tool_describe` — `tools/tool_search.py:44`
- `tool_call` — `tools/tool_search.py:45`

Core test at `tools/tool_search.py:157`: imports `_HERMES_CORE_TOOLS` and marks them as never-deferrable. Any tool not in `_HERMES_CORE_TOOLS` is deferrable (MCP + non-core plugin tools).

**Consolidation must-know:** Deferred tools have lower invocation counts by design (proxied through `tool_call`). Any "flag unused tools" feature must distinguish core-tool invocations (countable directly) from deferred-tool invocations (recorded under the actual name only after bridge dispatch — `tools/tool_search.py:21`).

---

## Q1 — Consolidation Recommendations: **YES**

A queryable source exists. Per-tool call data lives in the SQLite `messages` table:

- `messages.tool_name` — populated by the **gateway** on `role='tool'` rows (`agent/insights.py:201–203`)
- `messages.tool_calls` (JSON) — on `role='assistant'` rows; covers **CLI sessions** where `tool_name` is NULL

`InsightsEngine._get_tool_usage()` at `agent/insights.py:196–288` already queries both sources, merges with `Counter`, and returns `[{"tool_name": name, "count": count}]`. Skill invocations tracked separately in `_get_skill_usage()` at `agent/insights.py:290`.

A "never-used tool" query is a simple set-difference between `_HERMES_CORE_TOOLS` and `_get_tool_usage()` output. No new infrastructure required.

**Caveat:** Skill-activated tool invocations are not individually tracked — only `skill_view`/`skill_manage` calls are recorded. "Never-used skill" is answerable; "never-used skill tool" is harder.

---

## Q2 — Estimate-vs-Actual Reconciliation (bug #23767): **UNCERTAIN**

### Estimator

`estimate_tokens_rough(text)` at `agent/model_metadata.py:1897` — pure `(len(text)+3)//4`. Composed into `estimate_request_tokens_rough(messages, *, system_prompt, tools)` at `agent/model_metadata.py:1985`.

### Real provider count

`context_compressor.update_from_response(usage)` at `agent/context_compressor.py:771` — sets `self.last_prompt_tokens` and `self.last_real_prompt_tokens` (`context_compressor.py:773,777`) from the API response `{"prompt_tokens": N}`.

### The gap

`prompt_size.py` creates an **offline** agent; `last_real_prompt_tokens` is always 0 in that context. The two numbers only co-exist in `context_compressor.should_defer_preflight_to_real_usage()` (`context_compressor.py:785`) — a guard that uses the discrepancy internally but never surfaces it to the user.

**Reconciliation options and blast radius:**

| Option | Blast radius |
|---|---|
| Add `--live` flag to `prompt-size` (calls provider count-tokens endpoint) | High — per-provider handling; Anthropic has this endpoint, OpenAI/OpenRouter do not standardize it |
| Log estimate vs. actual in the conversation loop post-turn | Medium — touches `conversation_loop.py` and compressor, doesn't fix the offline `prompt-size` output |

---

## Q3 — Plugin Hook for `prompt-size` Sections: **NO**

`VALID_HOOKS` at `hermes_cli/plugins.py:128–170` defines 19 hooks — none related to `prompt_size` or prompt breakdown computation:

```
pre_tool_call, post_tool_call, transform_terminal_output, transform_tool_result,
transform_llm_output, pre_llm_call, post_llm_call, pre_api_request, post_api_request,
api_request_error, on_session_start, on_session_end, on_session_finalize, on_session_reset,
subagent_start, subagent_stop, pre_gateway_dispatch, pre_approval_request, post_approval_response
```

**What plugins already influence:** Plugin-registered tools (via `PluginContext.register_tool()`) are included in `agent.tools`, which `prompt_size.py:91` reads — so plugin tool schemas are already counted in the bytes total.

To let a plugin contribute new output sections to `prompt-size`, you'd need to add a hook name to `VALID_HOOKS` and call `invoke_hook(...)` from `compute_prompt_breakdown()`. Small, targeted core change.

---

## Existing Test Coverage

| Area | File | Count |
|---|---|---|
| `prompt-size` logic | `tests/hermes_cli/test_prompt_size.py` | 6 tests |
| `prompt-size` parser wiring | `tests/hermes_cli/test_subcommands_batch.py:75` | 1 parametrized case |
| Tool token estimation (tiktoken) | `tests/hermes_cli/test_tool_token_estimation.py` | 9 tests |
| `db.optimize_fts()` / `db.vacuum()` | `tests/test_hermes_state.py:TestOptimizeFts` | 4 tests |
| `sessions delete` / `sessions prune` CLI | `tests/hermes_cli/test_sessions_delete.py` | 4 tests |
| **`sessions optimize` CLI dispatch** | — | **0 — gap** |

---

## Upstream PR Landscape (NousResearch/hermes-agent)

| Gap | Upstream PR |
|---|---|
| Token estimates in `prompt-size` output | **None** — upstream took `/tokens` slash command route instead (#48470) |
| `sessions optimize` CLI integration test | **None** |
| `sessions optimize` undocumented | #34670 (open) |
| Q1 — Consolidation / flag unused tools & skills | **None** |
| Q2 — Estimate vs. actual reconciliation | #23934 (open) |
| Q3 — Plugin hook for `prompt-size` sections | **None** |

**Other relevant upstream PRs:**

| PR | Title | Relevance |
|---|---|---|
| #48470 | `feat(cli): add /tokens slash command for system prompt token breakdown` | Parallel approach to token visibility |
| #41461 | `fix(cli): pass platform toolsets to prompt-size inspection agent` | Fixes offline agent toolset mismatch |
| #41575 | `fix(prompt-size): respect enabled/disabled toolsets per platform` | Companion to #41461 |
| #41800 | `Reduce tool schema overhead with core tool deferral` | Touches `_HERMES_CORE_TOOLS` / Tool Search |
| #23934 | `fix(compression): separate provider-exact vs projected token state` | Addresses Q2 |
| #37571 | `feat(cli): add 'hermes prompt-dump' to print the full assembled system prompt` | Adjacent diagnostic |

---

## Complexity Estimates

| Extension | Complexity | Rationale |
|---|---|---|
| `sessions optimize` CLI integration test | **S** | One test file, no design decisions, no upstream competition |
| Token estimates in `prompt-size` output | **S** | `estimate_tokens_rough` already exists at `agent/model_metadata.py:1897`; purely additive to `compute_prompt_breakdown()` |
| Plugin hook for `prompt-size` sections (Q3) | **S** | Add one name to `VALID_HOOKS`, one `invoke_hook` call in `compute_prompt_breakdown()`, define return-value contract |
| Q1 — Consolidation / flag unused tools | **M** | Data pipeline exists; need cross-reference logic, deferred-tool distinction, new display surface |
| Q2 — Estimate vs. actual reconciliation | **L** | No offline meeting point; full fix needs per-provider token-count API or live call plumbing |

---

## Recommended First PR

**Add rough token estimates to `hermes prompt-size` output.**

**Why:** Directly addresses the user-visible symptom of bug #23767 (no token information in output). Purely additive — new `"tokens_rough"` sub-key alongside `"bytes"` in `compute_prompt_breakdown()`. No conflict with upstream #48470 (different surface: `/tokens` is a live slash command; `prompt-size` is an offline diagnostic). No live API call required.

**Blast radius:** `hermes_cli/prompt_size.py` only. One import (`estimate_tokens_rough` from `agent/model_metadata`). One new key per block in the JSON schema. One new render line per block. One new test assertion.

**What the PR must cover:**
- `compute_prompt_breakdown()` adds `"tokens_rough"` sub-key to each block, computed via `estimate_tokens_rough(block_text)`
- `render_breakdown()` prints `~N tokens` with `~` prefix to signal it is a heuristic
- Output note: `"Token estimates use ~4 chars/token heuristic; provider actual may differ"` — sets expectation against #23767
- One new test in `tests/hermes_cli/test_prompt_size.py`: `tokens_rough` key is present and positive when the system prompt is non-empty

This PR also defines the data contract that any future reconciliation PR (Q2, complexity L) would build on.
