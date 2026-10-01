### Deploy and schedule
- One bundle per pipeline (or per unit batch during fan-out). Bundle targets: `migration`
  (migration catalog + engagement warehouse) and `prod` (deployed only at cutover; `AGENTS.md`).
  Redeploys must converge; children redeploy after a partial failure rather than
  patching live resources.
- Every deployed job/pipeline lands with its schedule **PAUSED**. The cutover flip unpauses tested
  objects; it never deploys anything new.
- Migration and fan-out sessions deploy only to the `migration` target (`AGENTS.md`).

### Pipelines
- A legacy pipeline that relies on side-effect ordering (audit rows, sequence numbers) needs the
  ordering made explicit or the unit flagged; declarative pipelines reorder by inferred dependency.
- Expectations that drop rows change row counts; recon compares against legacy reject behaviour,
  not raw input counts.
- Legacy staging tables become temporary views only when nothing external reads them (check D4/D6
  first); otherwise they stay tables.

### Governance
- Legacy row-level security, masking, and retention are D8 dependencies: capture the legacy
  contract, implement as UC row filters / column masks, and include a masked-vs-unmasked recon
  check. Grants on published schemas are a cutover action (`AGENTS.md`).
- Managed vs external tables is decided per target profile before backfill; converting later
  moves data.
