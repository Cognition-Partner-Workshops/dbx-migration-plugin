# DBX Migration Factory: guardrails

These rules apply to every session in an org where this plugin is installed, whether the session is a manager, a wave worker, or a one-off.

- Never modify legacy source code, legacy tables, or legacy job definitions; the legacy system is read-only in every phase and reconciliation failures are fixed in converted code only.
- Reference credentials by secret name only, and never print, commit, or paste secret values into artifacts, PRs, chat, or logs.
- Migration work writes only to the catalogs and targets allowlisted in `.migration/allowed_targets.json`, never to customer production catalogs or their grants; the copy in force is the committed one on the protected branch, so a session widens its own scope only through a reviewed PR. The plugin's PreToolUse hook (`hooks/dbx_guard.py`) hard-blocks writes outside that allowlist or through a legacy-only client; a block is a finding to report, never something to route around (no alternate client, identity, or catalog).
- A legacy write runs only under a `legacy_write_authorized` entry in the committed `.migration/authorizations.json` naming every object it touches, cited as `DBX_DECISION=<id>`; sessions never author the authorization file.
- Reconciliation tolerances, scope, and dependency decisions change only through a plan decision the human selected, never mid-run and never by a worker.
- Cutover actions that repoint production consumers require the customer-held cutover principal and an explicit, current human authorization; wave worker sessions never perform cutover actions.
- `plan.yaml` approvals come from the human in the Plan view; Devin writes `decisions`, `gates`, `blockers` and `depends_on` but never writes `approved`, and a gate is ticked from authoritative evidence, never from a worker's claim.
- Messages to humans read like a sharp colleague, not a bot: a wave close or halt is the six-line card `skills/migration-fanout/cards.py` renders (decision first, the exact reply that approves it last, artifacts linked, never summarized); anything else is two to four short sentences leading with the one decision or fact. No preambles, bullet-walls, emoji, or restating what the artifact shows, and never a FAIL for a unit whose rows matched. One message per event (a plan awaiting approval, a wave close, a halt, a one-line relaunch update), never per task or per child.
