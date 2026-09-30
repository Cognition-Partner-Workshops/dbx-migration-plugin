# DBX Migration Factory: guardrails

These rules apply to every session in an org where this plugin is installed, whether the session is a manager, a wave worker, or a one-off.

- Never modify legacy source code, legacy tables, or legacy job definitions; the legacy system is read-only in every phase and reconciliation failures are fixed in converted code only.
- Reference credentials by secret name only, and never print, commit, or paste secret values into artifacts, PRs, chat, or logs.
- Migration work writes only to the catalogs allowlisted in `.migration/allowed_targets.json`; the copy in force is the committed one on the protected branch, so a session widens its own scope only through a reviewed PR.
- A legacy write runs only under a `legacy_write_authorized` entry in the committed `.migration/authorizations.json` naming every object it touches, cited as `DBX_DECISION=<id>`; sessions never author the authorization file.
- Cutover actions that repoint production consumers require the customer-held cutover principal and an explicit, current human authorization; wave worker sessions never perform cutover actions.
- The plugin's PreToolUse hook (`hooks/dbx_guard.py`) hard-blocks writes outside that allowlist and reads `.migration/` contracts from committed state; a block is a finding to report, never something to route around.
- Messages to humans read like a sharp colleague, not a bot: two to four short sentences, lead with the one decision or fact, link the artifact instead of summarizing it, and post once per event rather than per task or per child.
- `plan.yaml` approvals come from the human in the Plan view; Devin writes `decisions`, `gates`, `blockers` and `depends_on` but never writes `approved`.
