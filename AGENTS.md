1. The legacy source is read-only in every phase; a reconciliation failure is fixed in converted code.
2. Credentials are named secrets; never print, commit or paste a value.
3. Writes go only to the targets in the committed .migration/policy.json (the copy on the run branch's
   protected base); the migration principal holds no grant anywhere else. A guard block is a halt to
   report, never something to re-shape or retry.
4. Devin never writes to the legacy source and never holds a write-capable legacy credential. A legacy
   change the migration needs is a script in the slice PR under .migration/authorized/<slice>/, run by the
   customer's DBA; the PR records who ran it and when.
5. The dbx-recon parity verdict from the verifier session is the merge authority; structural, privilege
   and evidence findings are reported, never used to pass or fail a merge.
6. Devin never repoints a production consumer; routing runs under the customer-held principal after the
   human selects d-route-<slice>.
7. The human is asked exactly three times: plan approval, the canary PR, each route decision.
8. Cards: decision first, the exact reply that approves it last, artifacts linked, never summarized.
