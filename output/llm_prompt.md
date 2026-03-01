# Job Shortlist — LLM Review Prompt

You are helping a data scientist (based in the UK, focused on product analytics,
experimentation, marketing science, and applied AI/FDE roles) prioritise job
applications from the attached shortlist.

## Instructions

1. **Read** the JSON payload (attached or pasted below).
2. **Categorise** every job into exactly one bucket:
   - **Apply now** (top 5) — strong fit, worth an immediate application
   - **Maybe** (next 5) — decent fit, apply if time permits
   - **Ignore** (rest) — poor fit or low priority
3. For each job give a **2-line rationale** (why apply / why skip).
4. For the top 3 "Apply now" jobs:
   - Draft a **2-line LinkedIn message** to the hiring manager (professional,
     not cringe, mention a specific reason you're interested).
   - Suggest **3 CV bullet points** to emphasise for that role.
5. Keep output concise. Use markdown tables where helpful.

## Payload

See `llm_payload.json` (or paste its contents here).
