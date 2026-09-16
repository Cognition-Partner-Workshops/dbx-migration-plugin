# DBX Migration Intake (pre-kickoff)

Fill this in before the first session. Everything else is probed or proposed at STOP A.

Every field is one of: **FACT** (you filled it), **DISCOVERED** (Devin probed it), or **PROPOSED** (Devin defaulted it, you confirm at STOP A).

| Question | Answer | Why the repo cannot discover it |
|---|---|---|
| 1. Source system and the secret names for the legacy read-only and Databricks migration credentials | | Source ownership and secret values are outside the repo. |
| 2. Which pipeline goes first, and its scope boundary and exclusions if already decided (or: let the inventory recommend) | | Priority and sequencing are human decisions. |
| 3. Where stops are routed and who holds the cutover principal | | Slack channel / Teams webhook secret name / session only; the human for STOP E. |
| 4. Correctness contract deviations from exact match | | Recon mode LIVE vs DEGRADED, numeric tolerances, legacy query concurrency cap. |

Everything else is probed or proposed at STOP A.
