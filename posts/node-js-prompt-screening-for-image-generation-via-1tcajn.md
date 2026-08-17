# Node.js Prompt Screening for Image Generation via Schema-Constrained Chat

Short answer: place a fail-closed policy gateway before image-job creation, have a chat model return a three-state decision under a closed JSON Schema, validate that decision again in deterministic application code, and persist the accepted decision with an idempotency key before any image work begins.

The governing constraint is simple: a probabilistic classifier may advise, but it must never own the irreversible transition that creates a billable or externally visible image. In a Node.js application, the HTTP handler can remain thin while a language-neutral policy component owns canonicalization, classification, validation, audit persistence, and the decision to release or hold the job. There is no need for a dedicated moderation endpoint to preserve that boundary; there is, however, a need to treat chat output as untrusted input rather than as an authorization token.

This is an exactly-once problem in business terms, even though neither HTTP nor model inference promises an exactly-once execution primitive.

## What constraint should define prompt screening?

Begin at the commit point. Before that point, the system has a request that can still be rejected or held; after it, an image job exists and may consume capacity, produce regulated content, or become visible to another system. Prompt screening belongs immediately before that transition, after authentication and deterministic size checks but before job publication.

The policy result should have three outcomes: `allow`, `deny`, and `review`. A binary `safe` field is too weak because uncertainty then leaks into an arbitrary default. `allow` means no configured rule blocked the canonical prompt. `deny` means at least one stable policy rule did. `review` means the classifier or surrounding application lacks enough confidence or context to authorize the image. Unknown, malformed, contradictory, or stale results also become `review`; they do not become `allow` merely because the transport returned success.

Canonicalization needs restraint. Normalize transport-level details that the policy explicitly defines, such as line endings or a maximum accepted byte length, but don't casually lowercase, transliterate, or rewrite Unicode. Those operations may change meaning. Bind the exact canonical bytes to a prompt digest, and bind the decision to a policy version and classifier configuration identifier. If retention rules permit storage of the original prompt, restrict access and define a deletion schedule; if they do not, a digest and narrowly scoped decision metadata can still support reconciliation without preserving sensitive text indefinitely.

The audit record should answer a disputed decision without reconstructing state from application logs: which request was judged, under which policy and classifier configuration, what structured result was accepted, when it was accepted, and which image job, if any, followed. Logs are useful telemetry. They are a poor ledger because sampling, redaction, retention, and asynchronous delivery can all break the chain of evidence.

## How should Node.js validate chat JSON schema decisions before AI image generation?

Use the schema to narrow the wire format, then use local code to enforce meaning. A closed schema should require every field, reject additional properties, enumerate the three outcomes, constrain rule codes to a versioned catalog, and cap human-readable explanation length. The application must still reject cross-field contradictions, such as `allow` accompanied by blocking rule codes, because structural validity alone does not establish a valid policy decision.

The Node.js edge can submit the canonical prompt and policy version to a chat-capable interface that supports schema-constrained JSON, parse the returned object, and pass it to the policy boundary. The focused Go example below shows that boundary rather than a vendor-specific SDK or an invented HTTP route. A Node.js service can call the same logic through an internal service, or implement the identical contract locally; the important property is that only validated decisions can reach persistence.

```go
package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
	"time"
)

type Outcome string

const (
	Allow  Outcome = "allow"
	Deny   Outcome = "deny"
	Review Outcome = "review"
)

type Decision struct {
	Outcome   Outcome  `json:"outcome"`
	RuleCodes []string `json:"rule_codes"`
	Reason    string   `json:"reason"`
}

type AuditRecord struct {
	RequestID      string    `json:"request_id"`
	PromptDigest   string    `json:"prompt_digest"`
	PolicyVersion  string    `json:"policy_version"`
	ClassifierID   string    `json:"classifier_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Decision       Decision  `json:"decision"`
	DecidedAt      time.Time `json:"decided_at"`
}

func ValidateAndRecord(
	requestID, prompt, policyVersion, classifierID string,
	d Decision,
	now time.Time,
) (AuditRecord, error) {
	if requestID == "" || strings.TrimSpace(prompt) == "" ||
		policyVersion == "" || classifierID == "" {
		return AuditRecord{}, errors.New("missing canonical input")
	}

	switch d.Outcome {
	case Allow:
		if len(d.RuleCodes) != 0 {
			return AuditRecord{}, errors.New("allow cannot contain rule codes")
		}
	case Deny, Review:
		if len(d.RuleCodes) == 0 {
			return AuditRecord{}, errors.New("non-allow requires a rule code")
		}
	default:
		return AuditRecord{}, errors.New("unknown outcome")
	}

	sort.Strings(d.RuleCodes)
	promptSum := sha256.Sum256([]byte(prompt))
	digest := hex.EncodeToString(promptSum[:])
	keyMaterial := strings.Join([]string{
		requestID, digest, policyVersion, classifierID,
	}, "\x00")
	keySum := sha256.Sum256([]byte(keyMaterial))

	return AuditRecord{
		RequestID:      requestID,
		PromptDigest:   digest,
		PolicyVersion:  policyVersion,
		ClassifierID:   classifierID,
		IdempotencyKey: hex.EncodeToString(keySum[:]),
		Decision:       d,
		DecidedAt:      now.UTC(),
	}, nil
}
```

Strict decoding should reject unknown fields before `ValidateAndRecord` runs. Place a unique database constraint on `IdempotencyKey`, then insert the audit record and an outbox entry in one local transaction when the outcome is `allow`. The outbox publisher may run more than once, so the image-job consumer must deduplicate on the same logical request identity. That composition gives the business workflow exactly-once effects under retries without pretending that the network delivers a message exactly once.

Keep the schema's machine-facing fields stable. Free-form reasons help reviewers, but downstream code should branch only on the outcome and enumerated rule codes. Otherwise a wording change becomes an undocumented API change, audit queries become substring searches, and policy reconciliation becomes guesswork.

## Failure handling is part of the policy

An HTTP success response establishes only that an exchange completed according to the remote interface; it does not establish that the body matches the expected schema, that its policy version is current, that the audit transaction committed, or that the image reached a terminal state. Verify each boundary separately. RFC 9110 also distinguishes method semantics and cautions against automatically retrying a non-idempotent request unless the client knows that the request itself has idempotent semantics. Classification can be repeated, but a repeated probabilistic judgment need not be identical. Don't reclassify after an accepted decision merely because job publication needs a retry; preserve the accepted record and replay the outbox event. If classification times out, returns invalid JSON, names an unknown rule, or references the wrong policy version, hold the request for review or return a defined retryable application status before the commit point. A retry budget should be explicit, bounded, and observable, and after exhaustion the result remains unresolved rather than silently changing category. The catch is that chat-based screening is not suitable where a law, contract, regulator, or internal assurance case requires a certified or independently evaluated control. Use the mandated control in that environment while retaining the same local validation, idempotency, and audit boundary. It can also be the wrong choice when classification tail latency consumes the image workflow's entire latency budget; a deterministic prefilter plus asynchronous review may fit better. I'm not sure that any public aggregate benchmark can settle those choices for a specific workload, because the answer depends on language mix, adversarial behavior, local policy, and the relative harm of false allows and false denials. A labeled evaluation set drawn under the applicable data rules would resolve more than a generic score.

| Control | Appropriate use | Important limitation | Conservative failure posture |
|---|---|---|---|
| Deterministic rules | Input bounds and narrow, explicit prohibitions | Weak at semantic ambiguity and obfuscation | Reject malformed input; send ambiguity onward |
| Schema-constrained chat | Contextual classification under a local policy | Probabilistic judgment and configuration drift | Validate locally; hold invalid or uncertain results |
| Mandated moderation control | Compliance regimes that prescribe a control | Its taxonomy may not equal the local policy catalog | Map unmatched categories to review |
| Human review | High-impact, disputed, or low-confidence cases | Queue delay and reviewer inconsistency | Keep the image job on hold |

Layering matters more than picking a single row. Deterministic checks reduce obviously invalid traffic, contextual classification addresses meaning, and human review receives exceptions whose consequence justifies delay. The final authorization remains deterministic even when one input to it is probabilistic. Observability should follow the state machine: count outcomes by policy version, invalid structured responses, timeouts, duplicate suppressions, review-queue age, outbox lag, and elapsed time from request acceptance to terminal image state. Avoid placing raw prompts or unconstrained model explanations in ordinary logs, since both may contain sensitive material. Periodic reconciliation should join request, decision, outbox, and image-job records by stable identifiers and place any missing edge into an exception queue. A dashboard can describe a discrepancy; reconciliation gives it an owner.

## Roll out with evidence, then migrate compactly

Start with an observe-only candidate that cannot authorize image creation. Build a versioned evaluation set covering the actual policy boundaries, including obfuscation, multilingual input, quoted prohibited material, benign medical or historical context, and near-boundary cases. Qualified reviewers should label a governed sample, and acceptance thresholds should be category-specific because aggregate accuracy can conceal a rare but costly failure class.

Next, enforce deterministic input constraints, route uncertain decisions to review, and enable direct deny or allow transitions only after the audit transaction, deduplication behavior, and reconciliation job have been exercised. Pin the policy text, schema, rule catalog, and classifier configuration as one deployable version. Change one versioned component at a time when practical, shadow the candidate against the incumbent, and retain both decisions without allowing the candidate to affect production work.

Rollback means selecting the previous version for new requests. It must not rewrite historical decisions.

For migration, keep the Node.js handler contract narrow: submit canonical input with a stable request ID, accept only `allow`, `deny`, or `review`, expose distinct client states for rejection and pending review, and never enqueue directly. Backfill no synthetic decisions. Drain the old path, reconcile all accepted requests to terminal jobs, then remove it only when the exception queue is empty under the team's defined retention and compliance rules. That sequence is less exciting than embedding a model call in a route handler, and considerably easier to audit.

## References

- https://www.rfc-editor.org/rfc/rfc9110
- https://openrouter.ai/docs
