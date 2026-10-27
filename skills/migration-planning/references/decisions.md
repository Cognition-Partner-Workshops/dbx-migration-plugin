# Decision and blocker templates for plan.yaml

Every dependency crossing is a plan `decision` on the step that owns it (or on the phase when it
spans steps): `id` is a lowercase slug, `options` name the selectable answers, `selected` is the
chosen option id (null until decided), `important: true` flags the ones the human should weigh in
on before approving. Access/lead-time gaps are `blockers` with the machine `check` that proves
them closed (`kind: repo` | `kind: secret` | `kind: mcp`).

| Class | What it is | Options (option id : label) | Blocker shape |
|---|---|---|---|
| D1 | intra-pipeline lineage edge (ordering, not a decision) | none — expressed as `depends_on` between steps and wave order | none |
| D2 | shared object used by several pipelines (migrate once, first pipeline owns it) | `wave0` : migrate once in wave 0 under the owner pipeline | none |
| D3 | upstream feed owned by a system not migrating | `federate` : read-only federation (default); `connect` : managed Lakeflow Connect connector; `loader` : Auto Loader over exported change files | `check: {kind: mcp, server}` or a secret check for the connector credentials |
| D4 | downstream consumer (BI dashboard, report, extract, API) reading the legacy output | `repoint` : re-point at cutover; `dual` : dual-publish during coexistence; `rebuild` : rebuild on the target | none |
| D5 | scheduler / orchestration dependency (Control-M, Autosys, Airflow, cron) | `jobs` : replace with Lakeflow Jobs; `retain` : keep external scheduler triggering Databricks; `hybrid` : hybrid with completion signal | none |
| D6 | shared table written by both migrated and non-migrated writers | `dualwrite` : dual-write window; `legacy` : legacy remains writer + federated read; `defer` : documented deferral | none |
| D7 | external hand-off (SFTP drop, message queue, partner feed) | `preserve` : preserve the format contract exactly, re-platform the transport at cutover | none |
| D8 | security / governance contract (row-level security, PII masking, retention) | `uc` : reproduce in UC (row filters, masks, grants) before any consumer re-points | none |
| D9 | ML model or scoring consumer of the data | `parity` : prediction-parity gate per the ML-SCORING profile before re-pointing | none |
| D10 | environment/access dependency (network path, service principal, sample-data approval) | `fire` : fire the request now; track it as a `plan.yaml` blocker carrying the exact request text and its check, with reply or pending state on that blocker | `check: {kind: repo}` / `{kind: secret}` / `{kind: mcp}` proving the access exists |

Target state is recorded as one `target-*` decision per surface. A selected option names the
target and cites a reference implementation (which outranks a document) or a standards document;
an N/A option gives its reason. The wave manifest copies those selected decisions into
`target_state`.

Rules:

- Each entry records the full contract first (source, target, owner, lead time, evidence), then
  the decision, the routing point that flips traffic, and the cutover/decommission condition.
- D10 blockers fire at planning time: access requests routinely outlast the code work, and they
  gate fan-out width. A `secret` blocker closes when `databricks secrets list-secrets` would list
  it; a `repo` blocker closes when the repo is connected; an `mcp` blocker when the server is
  installed.
- Every fired request is a `plan.yaml` blocker carrying the exact request text and its check; the
  reply or pending state is recorded on that blocker.
- `important: true` is for choices with real blast radius — tolerances, coexistence mode,
  production-affecting sequencing — never for routine defaults.
- A waived wave gate is recorded on the gate itself: the manifest row carries `decision_id` of
  the plan decision that waived it.
- Serializing one pipeline across two batches is a D3-adjacent ordering decision: the manifest's
  `serialized_pipelines` names the decision slug whose selected option fixes the order.
