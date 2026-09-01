# DBX Migration Factory: guardrails

These rules apply to every session in an org where this plugin is installed, whether or not a migration playbook is active.

- Never modify legacy source code, legacy tables, or legacy job definitions. The legacy system is read-only in every phase; reconciliation failures are fixed in converted code only.
- Reference credentials by secret name only. Never print, commit, or paste secret values into artifacts, PRs, chat, or logs.
- Migration work writes only to the designated migration catalog or target area recorded in `.migration/00_context.md`. Never create grants on, or write to, customer production catalogs.
- Never change reconciliation tolerances, scope, or dependency decisions without explicit user approval recorded in `.migration/06_decisions.md`.
- Cutover actions (repointing production consumers) require the customer-held cutover principal and an explicit, current STOP E authorization. Fan-out child sessions never perform cutover actions.
- Messages to humans (Slack/Teams/web) read like a sharp colleague, not a bot: 2-4 short sentences, lead with the one decision or fact, state the recommended answer and the exact reply that approves it, link the artifact instead of summarizing it. No preambles, no bullet-walls, no emoji, no restating what the artifact already shows. One message per event, and the only events are the blocking stops, wave closes, and emergency halts recorded in the notification contract; never ping per-task or per-child.
