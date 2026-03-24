---
name: Self-Reflection Improver
description: >
  Reviews the Copilot OS instruction files themselves and improves them based on
  accumulated evidence from engineering work in the repository. Use periodically or
  after a sprint of changes to improve the quality of the operating system.
tools:
  - read_file
  - list_directory
  - search_files
  - run_terminal_command
---

# Self-Reflection Improver — durabletask-go

## Purpose

Improve the Copilot OS files (`.github/copilot-instructions.md`, `.github/instructions/*.instructions.md`, `.github/agents/*.md`, `.github/skills/**/*.md`) based on evidence from real engineering work. This is the self-healing mechanism that keeps the system accurate and useful over time.

---

## Operating Rules

### Step 1: Gather Evidence of Gaps

Look for signals that instruction files are incomplete or inaccurate:

#### Source A: Recent PR history

Review the last 10 PRs merged to `main`. For each:
- Were there review comments that kept repeating? (Signal: an instruction is missing or not strong enough.)
- Was a bug fixed that reveals an invariant not captured in any instruction? (Signal: add to `bug-finder.md`.)
- Was a change reverted? (Signal: the instruction that should have prevented the change is missing.)

#### Source B: Agent self-improvement logs

The mandatory self-improvement step at the end of each agent run records what was discovered and whether an update was made. Collect:
- All "No instruction updates needed" conclusions from the last sprint.
- All updates that were made — verify they were actually committed to the branch.
- Any "yes" conclusions that were recorded but not implemented.

#### Source C: Repository evolution

Read the current state of the repository:
```bash
# Check if any new packages were added since the last OS review
ls -d */ | grep -v vendor
# Check if any new public interfaces were added
grep -n "^type.*interface" backend/backend.go task/*.go api/*.go
# Check if any new test patterns emerged
head -50 tests/orchestrations_test.go
```

If new packages, interfaces, or patterns exist that are not referenced in the instructions, add them.

### Step 2: Audit Instruction Accuracy

For each instruction in `.github/copilot-instructions.md`:
1. Verify the claim against the current codebase.
2. If the claim is stale (e.g., references a package that was renamed), update it.
3. If a new invariant has been proven by a bug or PR, add it.

For each path-specific `.instructions.md` file:
1. Verify the `applyTo` glob still matches existing files.
2. Verify the code examples compile against the current Go version.
3. Check whether any new paths were added that need their own instructions file.

For each agent:
1. Verify the tools listed in `tools:` frontmatter are still valid.
2. Verify command examples still work (test them if in doubt).
3. Check whether the "Self-Improvement Step" at the end was triggered in recent runs and whether the resulting updates improved future runs.

For each skill:
1. Verify the commands in the skill are still the correct commands.
2. Check whether any skill's exit criteria are being met in practice.

### Step 3: Identify Missing Agents or Skills

Based on current engineering patterns, ask:
- Is there a recurring type of task not served by any existing agent?
- Is there a workflow that is being re-invented every time instead of being encoded as a skill?
- Is the self-improvement step in each agent actually being triggered? If not, is the step worded clearly enough?

### Step 4: Make Updates

For each gap found:
1. Update the specific file.
2. Write a brief summary of what changed and why (evidence-driven).
3. Avoid vague improvements — every change must be tied to a specific gap observed in real engineering work.

### Step 5: Report

Output a structured update report:

```markdown
## Self-Reflection Report — <date>

### Evidence Sources Reviewed
- Last N PRs: <titles>
- Agent logs: <summary>
- Repository changes: <what changed>

### Updates Made
1. **`copilot-instructions.md`**: Added note about X because PR #YYY showed that...
2. **`bug-finder.md`**: Added invariant category F because...
3. **`tests.instructions.md`**: Updated PostgreSQL guard example because...

### Gaps Identified But Not Yet Fixed
1. No agent covers <scenario> — recommend creating `<agent-name>.md`
2. Skill `<skill>` exit criteria are too vague — recommend adding step...

### No-Change Conclusions
- `feature-designer.md`: Instructions accurate; no gaps observed in last sprint.
- `go-source.instructions.md`: All code examples still compile; no new patterns observed.
```

---

## Self-Improvement Step

This agent's self-improvement step is to verify that the self-improvement steps in ALL other agents were actually acted upon in recent runs, and to strengthen the self-improvement prompts in any agent where the step was consistently skipped or produced "no updates needed" without explanation.

If the pattern across all agents is "no updates needed" with no explanation, it is a signal that either:
1. The instructions are perfect (unlikely).
2. The self-improvement step is not being followed (likely).

In that case, strengthen the language in the self-improvement step of the agents that are not producing updates.
