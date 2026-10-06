# Canonical phase/step skeleton

A new `plan.yaml` starts from this skeleton; the manager renames, splits, and adds decisions,
gates and blockers per the estate. Step ids are slugs; `depends_on` names earlier step ids.

- Phase `intake` — the manager's checklist (`skills/intake/SKILL.md`); keep the step for
  traceability with the request's answers recorded.
- Phase `foundation`
  - `workspace-setup` — worker ticket invoking `skills/workspace-setup`: writes the committed
    `.migration/` workspace (`allowed_targets.json`, `recon_tolerances.json`,
    `capabilities.json`), runs the probes and the doctor, and reports blocked items back as
    plan blockers.
  - `scaffold-wave-0` — catalog/schemas, federation or backfill, CI, harness install, bundle/job
    shells; serial (its wave manifest has `width: 1`), `depends_on: [workspace-setup]`.
  - `access-requests` — every fired request is a `plan.yaml` blocker carrying the exact request
    text and its check; the reply or pending state is recorded on that blocker. Depends on
    nothing but blocks later phases.
- Phase `waves`
  - `pilot` — wave 1, width ≤ 5; one unit of each new pattern class runs narrow before fan-out.
  - `run-wave-<N>` — one step per wave, `depends_on` the previous `run-wave-*` step (merge order
    is wave order) plus any `scaffold`/`access` steps it needs. The step's id is the manifest's
    `plan_step`; its gates name the wave's acceptance gates.
- Phase `coexistence`
  - `parallel-run` — scheduled recon job, remediation rules, green-cycle counter.
- Phase `cutover`
  - `cutover` — evidence pack, independent audit, consumer rehearsal, repoint. Always gated on
    the human's explicit authorization; `important` decisions live here.

Gate rules:

- A gate is `done` only from authoritative evidence (verdict JSON, merged PR, job run) — the
  manager ticks it, never a worker's claim.
- Waived gates keep the plan decision id that waived them (`decision_id` on the manifest row).
