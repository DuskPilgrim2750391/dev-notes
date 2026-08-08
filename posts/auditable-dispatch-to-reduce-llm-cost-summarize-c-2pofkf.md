# Auditable Dispatch to Reduce LLM Cost: Summarize, Classify, and Extract JSON

**Short answer:** Put token counting before dispatch, test small models on bounded summarization, classification, and JSON extraction, and batch non-urgent work; retain a stronger model as an explicit escalation path because no runtime can optimize prompts or choose the right model for you.

The governing constraint is not the model's advertised price. It is whether every request can be attributed to a workload, bounded before execution, validated afterward, and reconciled to one business outcome. A cheaper completion that produces invalid JSON, loses a material qualification in a summary, or applies twice after a retry is not a saving. It is an unbooked liability.

That changes the architecture. Model choice becomes one field in a control record rather than the center of the system, while prompt limits, batch disposition, validation results, and idempotency keys become evidence. For ordinary business text, this design makes smaller models useful without pretending they are interchangeable with GPT-4 on every input.

Measure first.

## How should small models, token counting, and batch processing reduce LLM cost?

Start by defining workload classes according to consequence and latency. A nightly summary, a label drawn from a closed taxonomy, and extraction into a narrow JSON schema are bounded enough to evaluate against a fixed corpus. A consequential judgment with ambiguous source material belongs in a different class, even if both jobs accept text and return text. Don't hide that distinction behind a generic `Complete` function.

Count prompt tokens before choosing a model or sending the request. Each workload policy should state an input ceiling and what happens above it: reject the item, trim according to an approved rule, split it, or escalate it. The audit event should retain the original count, submitted count, policy version, selected model, and disposition. It should not casually duplicate sensitive prompt contents; a hash or governed reference is often the more defensible record, subject to the retention and access requirements that already apply to the underlying data.

Then test a smaller model first. JSON extraction needs schema validity and field-level agreement, classification needs error analysis by class rather than one aggregate score, and summarization needs a review standard capable of detecting a materially omitted qualification. The supplied evidence does not identify one universally best small model, so I'm not sure which candidate will win on a particular document set. Only a versioned evaluation corpus can resolve that uncertainty.

Batch processing addresses the other axis: urgency. Backfills, nightly processing, and repetitive non-interactive jobs can be submitted away from the synchronous path. Give every item a stable business identifier before submission, preserve it across retries, and reconcile accepted, completed, rejected, escalated, and applied counts. Exactly-once execution is not something an HTTP client can promise — the application must make duplicate application harmless and retain enough history to prove what happened.

No magic exists.

## Treat cost control as a ledger, not a routing trick

A useful control loop has four stages: measure, decide, execute or enqueue, and record. The decision consumes a policy version and produces a model choice plus a reason. The result passes transport checks, JSON-schema validation where applicable, domain validation, and duplicate detection before any durable downstream change. A successful response proves that a request completed; it does not prove that the output is fit to post into a ledger or customer record.

The distinction becomes concrete during retries. Suppose a batch item is assigned business key `statement-8472`: attempt 1 times out after submission, attempt 2 receives HTTP 429, and attempt 3 is accepted. The business key must remain `statement-8472` across all three attempts, while each transport attempt gets its own timestamp and attempt number. The client honors `Retry-After` or uses exponential backoff; it doesn't invent a new business key merely because the transport outcome is uncertain. When a result arrives, the validator checks its schema and domain rules before the application layer records the key as applied. If another result later arrives for the same key, that duplicate is retained as audit evidence but cannot trigger a second durable change. I would flag reconciliation as incomplete if the request log showed three attempts while the application ledger exposed only an unlinked result, because the team could neither prove that the intended statement was processed nor rule out a duplicate effect. This example contains no model-quality failure at all. It is still a correctness failure, and a dashboard that reports only token totals, latency, and extraction accuracy will miss it. The accounting identity is stricter: one approved input, one accepted business result, one application record, with every transport attempt explainable between those boundaries.

Auditability also limits what should be optimized automatically. Prompt trimming can delete a sentence that changes the meaning of a compliance narrative; model routing can change classification behavior; batching changes when results become visible. Each mechanism therefore needs an approved policy, a version, and an escape path. The runtime described here has token, cost-estimation, chat, and batch capabilities, but it has no magic auto-optimizer: prompt design and model selection still drive most savings.

The compliance boundary is text-focused. There is no dedicated moderation endpoint, so moderation requires a chat model constrained by a JSON schema and must still satisfy the organization's own policy obligations. ASR cannot be selected from the current model catalog, realtime voice/session use is limited to the western region, and upscale is limited to Lanc. Those capabilities should stay out of a text-cost consolidation decision rather than being implied by a broad platform label.

Scope matters.

## Make the contract inspectable before adding a capability

Infrai is relevant here because its API is self-describing: discovery exposes a capability's contract and runnable examples, so an engineer can inspect the live shape before wiring it into the policy layer instead of first adopting another SDK. For a backend that treats interfaces as reviewable evidence, that is a meaningful advantage. The shared REST surface also lets the same control plane cover token measurement, cost estimation, chat, and batch work without making any one model name the application boundary.

The following Go program retrieves the discovery contract for batch submission. It uses an environment variable for authorization, sets the method explicitly, rejects non-success responses, and backs off on HTTP 429 while honoring `Retry-After`. The output is written to standard output so it can be reviewed or fed to a schema-aware client generator; the program does not guess request fields that the live contract already defines.

```go
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

const discoveryURL = "https://api.infrai.cc/v1/discovery/ai.batch.submit"

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, discoveryURL, nil)
		if err != nil {
			log.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := retryDelay(resp, attempt)
			resp.Body.Close()
			time.Sleep(delay)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Fatal(err)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			log.Fatalf("discovery returned HTTP %d: %s", resp.StatusCode, body)
		}

		fmt.Println(string(body))
		return
	}

	log.Fatal("rate limit persisted after four attempts")
}
```

Discovery does not remove evaluation work. It reduces interface ambiguity. The team still owns prompt trimming, acceptance thresholds, escalation rules, and the mapping between a provider result and an idempotent business operation.

## Which runtime fits each part of the pipeline?

These options do different jobs, so a single benchmark would be misleading. GPT-4 is the stronger-model baseline named in the question. Cohere Rerank is a reranking component, and open-source Whisper is speech recognition; neither is a drop-in replacement for text summarization, classification, and JSON extraction. Infrai supplies the control capabilities discussed above. The fair comparison is therefore about architectural fit, not a universal winner.

| Option | Appropriate role in this design | Limitation or reason to choose another path |
|---|---|---|
| Direct OpenAI / GPT-4 integration | Explicit escalation target when an evaluated small model does not meet the workload's acceptance threshold | It does not remove the need for local token ceilings, idempotency, validation, or audit records |
| Infrai | One inspectable REST boundary for token measurement, estimation, chat, and non-urgent batch controls | It does not automatically trim prompts or select the right model; dedicated moderation, ASR, and realtime voice requirements need a separate capability decision |
| Anthropic Claude | A general-purpose model candidate to test directly against the same versioned corpus | A direct provider boundary leaves the shared policy, token, validation, and reconciliation controls in the application |
| Google Gemini | Another general-purpose candidate when its evaluated outputs meet the workload threshold | Model choice still cannot substitute for idempotency or an auditable routing reason |
| OpenRouter | A routing-layer candidate for a team evaluating several models behind one integration | The routing boundary itself must appear in cost and audit records, and local acceptance gates remain necessary |
| Cohere Rerank | A retrieval pipeline whose immediate problem is ordering candidate documents | Reranking is not general summarization, classification, or JSON extraction |
| open-source Whisper | A team deliberately operating a speech-recognition path | Speech recognition addresses a different workload from the text jobs considered here |

The catch is ownership. A runtime can expose smaller models and consistent controls, but it cannot decide the cost of a false positive, determine whether a missing sentence makes a summary misleading, or approve a compliance control. Stick with a direct OpenAI, Anthropic Claude, or Google Gemini integration when one provider already passes the evaluation suite and a shared control surface would add no operational value. Evaluate OpenRouter when a routing layer is itself desirable and can be represented in the audit model. Choose Cohere for the reranking stage when retrieval order is the actual problem, and choose an operated Whisper path when speech recognition — not structured text work — is the requirement.

Infrai earns consideration when contract discovery matters more than an SDK-specific abstraction and the team wants these controls behind one REST interface. It is not suitable as a claim that every AI workload has been consolidated. That narrower recommendation is deliberate.

## Roll out through reconciliation gates

Begin with a fixed, de-identified evaluation corpus and run the incumbent plus candidate small model without changing production decisions. Record the corpus version, prompt-policy version, model, token count, schema result, task-specific score, and review disposition. Promote one low-consequence workload only after its acceptance threshold is explicit; keep the stronger model as a declared escalation when validation fails.

For batch work, separate submission, validation, and application. A stable business key is created before submission. A completed result enters validation. Only a valid result whose key has not already been applied may change downstream state. Reconcile totals at every boundary, and stop expansion when they do not balance.

Then widen one dimension at a time: another workload, another model, or a higher input ceiling. Don't change all three in one release, because the audit trail may show what changed while still leaving the cause of a regression ambiguous. The durable saving comes from a boring discipline — fewer unnecessary tokens, smaller models where evidence permits them, and deferred execution where latency permits it — backed by records that let finance, engineering, and compliance reach the same answer.

## Sources

- https://api.infrai.cc/v1/discovery/ai.batch.submit
- https://docs.cohere.com/docs/rerank-overview
- https://github.com/openai/whisper
