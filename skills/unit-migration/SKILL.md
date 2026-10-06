---
name: unit-migration
description: The worker ticket that converts one fan-out batch of units, proves parity with the recon harness, and opens one evidence-backed PR. The migration-fanout workflow embeds this skill verbatim in every child prompt; load it for conversion work inside a wave batch.
---

# unit-migration

You own exactly the units and write targets your batch names — nowhere else. Anything missing
from the brief stops you: report blocked, never improvise.

## Ticket inputs (cold start)

Batch id, units, targets, brief, capability contract, source block, `doctor_max_age`,
`max_minutes`, declared gates; each unit's `.migration/units/<id>/` handoff (`mapping_spec.json`
declares `rerun_posture`); target=`manifest.target_state`; the signed
`.migration/waves/wave-<TAG>.doctor.json`;
`skills/data-reconciliation/SKILL.md` (verdict authority); `skills/target-routing/SKILL.md` (auth).

## Procedure

1. Preflight: `factory-doctor --role child --reuse-record` with the manifest's source flags; a
   `fail` row stops you (name the check id); a non-fatal row goes in your summary.
2. Convert each unit with CORE + workload profile + dialect skill; implement only the decided
   dependency mechanisms; each derived rule is a `skill_feedback` line.
3. Deploy only to the isolated namespace, idempotently, jobs via bundle with schedule PAUSED;
   platform 5xx follows the bounded retry (twice: 30 s, 120 s).
4. Reconcile fixture-first (missing endpoints fail closed), read the real source once; wave 0
   proves fixture shape first; rerun proof via `dbx-recon rerun-proof`. Read `parity` and
   `merge_policy` separately: a `data` blocker you fix in converted code; `structural`,
   `privilege_visibility`, `rerun_policy` and `evidence` blockers you report by class.
5. Cap recon re-runs at the contract's; never adjust agreed thresholds. The last failure gets a
   one-word `failure_class`; "identity"/"sequence" means parent-owned `resync`.
6. Open your single PR (verdict on line 1, recon JSON linked); merging happens at wave close.

## Evidence the ticket must attach

PR URL and changed paths; each unit's `result.json` + `report.md` under its recon evidence dir;
gate evidence files; `recon_cost`; `skill_feedback`; a one-line human summary.

Full procedure: `references/procedure.md`.
