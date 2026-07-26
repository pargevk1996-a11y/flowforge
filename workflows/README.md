# Reference workflows

Three real scenarios that exercise the whole engine, not an abstract demo. They
land on top of the durable core as the LLM step, triggers, and fan-out
subsystems come online (see the root README roadmap).

- **`invoice_to_payment/`** — email → OCR → typed LLM extraction → vendor match →
  `WaitForApproval` (CFO) above $10k → create payment → Slack notify, with a
  compensation on every side-effecting step.
- **`support_triage/`** — Zendesk webhook → LLM classify → RAG → draft →
  `WaitForApproval` when confidence < 0.8 → send → 48h follow-up if the customer
  goes quiet.
- **`contract_review/`** — fan-out by paragraph → parallel LLM risk checks →
  fan-in → summary report → legal approval.

Each directory will hold the workflow definition, its Pydantic input/output
contracts, and the activities it calls.
