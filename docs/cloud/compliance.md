# Compliance & Certifications Roadmap

## Guiding Principle

Certifications are not a checkbox exercise — they are a forcing function for building a security program that would exist anyway. Start building toward SOC 2 controls from the first day of cloud development. Retrofitting compliance is expensive and slow; designing for it is not.

The E2E encryption architecture is a structural compliance asset, not just a user feature. It simplifies audit scope, strengthens control narratives, and lets us make claims most vendors cannot.

---

## Priority 1: SOC 2 Type II

**Why:** The universal gating requirement for enterprise sales. Any company with a security review process will ask for it before signing a SaaS contract. Without it, the Enterprise tier cannot close deals.

**Two stages:**

| Stage | What it is | Timeline |
|---|---|---|
| Type I | Point-in-time snapshot of controls design | ~6 months post-launch |
| Type II | Same controls assessed over 6–12 months of operation | ~18 months post-launch |

Type I is sufficient for early enterprise conversations. Type II is what procurement teams require before signing.

**Trust Service Criteria to cover:**
- **Security** (CC series) — required for any SOC 2 report
- **Confidentiality** (C series) — directly supported by E2E encryption
- **Privacy** (P series) — directly supported by hard deletes, export, zero data sharing

**How the encryption model simplifies the audit:**

Most vendors must demonstrate employee access controls, data handling procedures, and key management policies. Our architecture makes several of these controls provable rather than procedural:

| SOC 2 Control | Standard vendor approach | Our approach |
|---|---|---|
| CC6 — Logical access to data | Access control lists, employee policies | Encryption key never reaches server; access is cryptographically impossible |
| C1.1 — Confidentiality of data | Encryption at rest + TLS | AES-256-GCM per-entry client-side encryption; server holds ciphertext only |
| P4 — Use of personal information | Policy + audit | Content never processed server-side; cannot be used even if we wanted to |
| P6.6 — Deletion of personal information | Soft-delete + schedule | Hard delete, synchronous, verified by test suite |

This audit story is unusual. Lead with it.

**Implementation tooling:** Use Vanta, Drata, or Secureframe from day one. These tools integrate with AWS/GCP/Azure, GitHub, and HR systems to automate ~60% of evidence collection and flag control gaps continuously. Cost: $10K–$30K/year. Far cheaper than manual evidence gathering at audit time.

**Penetration testing:** Required for SOC 2 Type II. Commission an annual third-party pen test. Publish a responsible disclosure / bug bounty policy before launch (HackerOne or Bugcrowd, or a simple security.txt + email).

---

## Priority 2: ISO 27001

**Why:** The internationally recognized Information Security Management System standard. Required by many European enterprise customers and taken more seriously than SOC 2 in EU/UK/APAC markets. Opens doors SOC 2 alone does not.

**Overlap with SOC 2:** ~65% of controls are shared. Once SOC 2 Type II is complete, ISO 27001 is the natural next step rather than a fresh lift. The compliance tooling (Vanta/Drata) supports both simultaneously.

**Timeline:** Begin after SOC 2 Type II is in hand. Certification typically takes 6–12 months.

---

## Priority 3: ISO 27701

**Why:** The privacy extension to ISO 27001 — formally certifies a Privacy Information Management System (PIMS). Given the zero-knowledge architecture, this is unusually achievable for us and would be a strong differentiator. Most SaaS vendors cannot credibly claim it because their data access patterns make several controls difficult to satisfy. We satisfy them architecturally.

Maps directly to GDPR requirements and is increasingly cited in EU procurement.

**Timeline:** Pursue in parallel with or immediately after ISO 27001.

---

## Priority 4: EU–U.S. Data Privacy Framework (DPF)

**Why:** If the company is US-based and serves EU customers, a legal mechanism is required to transfer EU personal data to US servers. DPF is the current framework (successor to Privacy Shield, effective 2023). Without it, every EU customer contract requires Standard Contractual Clauses — legal overhead on every deal.

**How:** Self-certification through the U.S. Department of Commerce. Not a third-party audit — a self-assessment and public commitment. Low cost; requires a privacy policy and internal practices that should exist anyway by this point.

**Timeline:** Before onboarding EU customers. Can be done in parallel with SOC 2 Type I work.

---

## Do Early, Zero Cost: CSA STAR Level 1

The Cloud Security Alliance STAR registry is publicly searchable. Security-conscious buyers (and their security teams) check it. Level 1 is a free self-assessment using the Consensus Assessments Initiative Questionnaire (CAIQ) — it maps cloud-specific security controls and takes 2–4 days to complete.

Complete before or shortly after public launch. Signals seriousness; costs nothing.

---

## Certifications to Defer

| Certification | Reason |
|---|---|
| **FedRAMP** | US federal contracts only; costs $500K–$2M+ and 12–24 months; pursue only with a specific federal opportunity in hand |
| **HIPAA** | No health data; not applicable |
| **PCI DSS** | Stripe handles all cardholder data; our PCI scope is negligible (SAQ A) |
| **SOC 1** | Financial reporting controls; not relevant to our product |
| **TISAX** | Automotive industry; not applicable |

---

## Recommended Timeline

| Milestone | Target |
|---|---|
| `security.txt` + responsible disclosure policy | Before public launch |
| CSA STAR Level 1 self-assessment | Before or at public launch |
| EU–U.S. DPF self-certification | Before first EU customer |
| Compliance tooling live (Vanta/Drata) | Phase 1 cloud development |
| First third-party penetration test | Before SOC 2 Type I |
| SOC 2 Type I (Security + Confidentiality + Privacy) | ~6 months post-launch |
| SOC 2 Type II | ~18 months post-launch |
| ISO 27001 | After SOC 2 Type II |
| ISO 27701 | Alongside or after ISO 27001 |

---

## What to Say to Enterprise Prospects Before Certifications Land

SOC 2 Type II takes 18 months. Enterprise deals will come before that. In the interim:

1. **Share the encryption architecture** — the zero-knowledge design is auditable by reading the open source client. A skeptical security team can verify the privacy claim themselves.
2. **Provide a security questionnaire response** — most enterprise procurement uses a standardized questionnaire (CAIQ, SIG, or VSAQ). Completing one proactively (supported by CSA STAR Level 1) covers most of what they ask.
3. **Offer a data processing addendum (DPA)** — standard GDPR contract artifact; signals you take compliance seriously even without certifications yet.
4. **SOC 2 Type I in progress** — being able to say "we have engaged [auditor] and are targeting Type I by [date]" satisfies many security teams at the evaluation stage.
5. **Pen test report** — commission the first pen test early; sharing the report (with findings remediated) builds significant trust.

The E2E encryption story is the strongest card at this stage. No certification required to say "even we cannot read your data" and prove it with code.
