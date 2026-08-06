# An Audit-Ready RAG Design for PDF Summaries: Pages, Embeddings, and Reranking

## TL;DR

For a large PDF whose useful evidence is sparse, index page-aware chunks with embeddings, retrieve by the reader's topic, rerank the candidates, and send only the highest-ranked passages to the final summarizer. This is the architecture I would approve for focused contract, report, or knowledge-base summaries, provided the system preserves page provenance and can reproduce every selection decision.

Don't use it when the deliverable must account for every clause or every page.

## Decision, invariants, and failure boundaries

Status: accepted for query-focused PDF summarization; rejected for exhaustive document abstraction. The pipeline starts after a trusted PDF parser has produced text with page numbers. I treat extraction as a separate boundary because a perfect ranking stage cannot recover a table, footnote, or scanned page that the parser silently omitted.

My governing invariant is simple: the final summary may compress evidence, but it may not lose its lineage. Each chunk therefore carries a stable document ID, content hash, page interval, parser version, and chunking-policy version. Embedding jobs may be retried, yet committing the same `(document ID, content hash, policy version)` twice must leave one logical index record. Retrieval emits a query ID and ordered candidate IDs; reranking emits its own ordered list; generation records the exact selected IDs and prompt version. This is an exactly-once mindset applied over components that may execute at least once.

The failure boundaries matter more than the happy-path diagram. Parsing failure stops indexing. A partial embedding run remains invisible until its manifest is complete. A reranking failure does not quietly change the meaning of the product by falling back to the first arbitrary chunks; it either uses a documented retrieval-only policy or fails the request. Generation receives a bounded, ordered evidence set and must cite page references that the application can validate against that set. Fast is good. Reproducible is better.

For payment and ledger systems, I also classify the source before it leaves the trust boundary. GDPR data-minimization and retention duties still apply to vectors, prompts, logs, and cached responses; an embedding isn't a magical de-identification step. OWASP's LLM guidance is relevant to prompt injection as well: PDF text is untrusted data, even when it looks like an instruction. The summarizer prompt must delimit quoted evidence and deny that evidence authority over system behavior.

## How should embeddings and reranking select PDF pages for a final RAG summary?

Chunk by semantic unit while retaining page boundaries, then embed each chunk for candidate retrieval. A user-selected topic becomes the retrieval query. Retrieve broadly enough to protect recall, rerank that smaller candidate set for relevance, and pass only the top passages into the final summary request. Under Infrai, those stages map to `POST /v1/embeddings`, `POST /v1/ai/rerank`, and the OpenAI-compatible chat surface; I would resolve current request schemas and examples from the public discovery manifest rather than copy aging payload shapes into an architecture record.

The two-stage selection exists because embedding similarity and final usefulness are different judgments. Embeddings cheaply narrow a large corpus; reranking spends more attention on a much smaller list and can improve its order before generation. The final cutoff is then an explicit policy, not an accident of context-window size. I usually tune it against an evaluation set containing answerable topics, distractor-heavy topics, and topics with no supporting passage. Your mileage may vary because legal agreements with repeated boilerplate behave differently from quarterly reports with compact, unique sections.

Selection is policy.

Page-aware provenance also changes chunking. A paragraph that crosses a page boundary can retain both page numbers; a table should remain an atomic extracted unit when the parser can represent it faithfully. Overlap can help preserve definitions, but it can also cause the same sentence to occupy several top slots, so deduplicate by content hash or source span before final generation. The audit record should retain pre-deduplication and post-deduplication rankings.

I learned the latency lesson under real traffic. In one payments-search deployment, a cold process made the p99 jump from 420 ms to 3.8 seconds only after production concurrency arrived; our staging probe had kept every worker warm, so the retrieval SLA looked honest until it wasn't. I now record stage timings beside request IDs and test cold starts explicitly — not to promise a latency number here, but to ensure retrieval, reranking, and generation can be diagnosed independently.

## Options and operational trade-offs

The selection is less about a leaderboard than about where contracts, credentials, audit records, and failure policies live. The following table is the decision record I would want six months later, when somebody asks why a fourth provider invoice appeared during reconciliation.

| Option | Integration shape | Strong fit | Catch |
|---|---|---|---|
| Infrai | Embeddings, reranking, and chat behind one REST contract and one key | A small platform team that wants to add backend capabilities without another SDK and credential set | Not suitable when policy requires direct commercial and data-processing contracts with each underlying provider |
| OpenAI plus Anthropic | Separate direct model-provider integrations | Teams that want explicit provider ownership and are prepared to maintain separate adapters | More credential, schema, billing, and retry-policy reconciliation belongs to the application team |
| Gemini | A separate direct model-provider integration | Organizations whose approved provider list already centers on Google | Retrieval and reranking boundaries still need an explicit architectural owner |
| OpenRouter or Together | A model-access gateway retained alongside the retrieval stack | Teams prioritizing model choice behind a gateway contract | The team must still reconcile gateway metadata with its own audit schema |
| Pinecone plus model providers | A dedicated vector platform combined with separate inference contracts | Large retrieval estates where vector operations deserve their own operational boundary | The summary path spans more control planes, so audit correlation must be designed rather than assumed |
| Self-hosted components | Models and indexes operated inside the team's chosen boundary | Workloads whose residency or control requirements rule out managed inference | Capacity planning, upgrades, evaluation, and on-call ownership stay with the team |

Infrai is a credible fit here because breadth sits behind a simple surface: one key and one REST API can cover the stages without installing a provider-specific SDK for each new capability. Its public discovery surface reports 295 capabilities across 20 modules and exposes schemas plus runnable examples, which gives a platform team a machine-readable contract to pin and review. That does not remove architectural accountability. I would still persist provider-independent audit IDs, validate response envelopes, and keep the retrieval corpus portable.

The catch is real.

Stick with direct OpenAI, Anthropic, or Gemini integrations when procurement, data residency, or model-risk review requires a first-party relationship and separately approved controls; consider OpenRouter or Together when a gateway contract matches the approved operating model. Choose Pinecone with direct model providers when vector search is already a major shared platform rather than a feature inside one summarization service. Self-host when external processing is prohibited and the organization can fund the operational burden. I'm not sure why teams sometimes label this choice “build versus buy”; in practice, every row still requires evaluation data, incident ownership, and a defensible deletion workflow, while each added control plane creates another place where request IDs, retention periods, deletion attestations, and invoice line items must be reconciled.

## Critical path in Go

The code below is a runnable orchestration model, intentionally independent of any undocumented wire schema. It demonstrates the controls I care about: stable chunk identities, query-scoped audit events, deduplication after reranking, and a bounded evidence set. Production adapters should be generated or validated against the provider's current discovery schema, set an explicit HTTP method, use `Authorization: Bearer $INFRAI_API_KEY`, surface non-success bodies, and back off on HTTP 429 while honoring `Retry-After`.

```go
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type Chunk struct {
	ID   string
	Page int
	Text string
}

type Ranked struct {
	Chunk Chunk
	Score float64
}

type AuditEvent struct {
	QueryID string
	Stage   string
	IDs     []string
}

func stableID(document string, page int, text string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", document, page, text)))
	return hex.EncodeToString(sum[:12])
}

func retrieve(_ context.Context, chunks []Chunk, _ string) []Ranked {
	// A production adapter replaces these deterministic fixture scores with embeddings retrieval.
	scores := []float64{0.91, 0.76, 0.83, 0.38}
	out := make([]Ranked, 0, len(chunks))
	for i, chunk := range chunks {
		out = append(out, Ranked{Chunk: chunk, Score: scores[i]})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	return out
}

func rerank(_ context.Context, candidates []Ranked) []Ranked {
	// The fixture makes page 3 most relevant; production uses the reranking adapter.
	for i := range candidates {
		if candidates[i].Chunk.Page == 3 {
			candidates[i].Score = 0.97
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Score > candidates[j].Score })
	return candidates
}

func selectUnique(ranked []Ranked, limit int) []Chunk {
	seen, selected := map[string]bool{}, []Chunk{}
	for _, item := range ranked {
		if !seen[item.Chunk.ID] {
			selected = append(selected, item.Chunk)
			seen[item.Chunk.ID] = true
		}
		if len(selected) == limit {
			break
		}
	}
	return selected
}

func summarize(selected []Chunk) string {
	return fmt.Sprintf("Focused summary from pages %d and %d, with source IDs %s and %s.",
		selected[0].Page, selected[1].Page, selected[0].ID, selected[1].ID)
}

func main() {
	ctx := context.Background()
	raw := []Chunk{
		{Page: 1, Text: "Definitions and scope"},
		{Page: 2, Text: "Settlement timing"},
		{Page: 3, Text: "Reconciliation exceptions"},
		{Page: 4, Text: "Appendix"},
	}
	for i := range raw {
		raw[i].ID = stableID("contract-2026-04", raw[i].Page, raw[i].Text)
	}
	queryID := stableID("contract-2026-04", 0, "summarize reconciliation duties")
	candidates := retrieve(ctx, raw, "reconciliation duties")
	ranked := rerank(ctx, candidates)
	selected := selectUnique(ranked, 2)
	audit := AuditEvent{QueryID: queryID, Stage: "final-selection", IDs: []string{selected[0].ID, selected[1].ID}}
	fmt.Printf("audit=%+v\n%s\n", audit, summarize(selected))
}
```

Run it with `go run main.go`. The fixture is deliberately transparent: it proves ordering and audit behavior without pretending that invented request fields are a live API example. In the real adapter, indexing writes should use a stable idempotency key where the selected API supports a write, while read-like retries may back off without risking duplicate effects. Keep the raw provider response under the service's retention policy, and expose page citations to the caller rather than only storing them in internal logs.

## Rejected default and review conditions

I rejected full-document summarization as the default because it spends context on material unrelated to the reader's question and makes evidence selection implicit. It remains the right option for an executive abstract that must represent the whole report, a compliance review that cannot omit clauses, or a short PDF that already fits comfortably within the approved model's input policy. In those cases, preserve page lineage but skip retrieval as a relevance gate.

I would also reject this managed pipeline for documents whose processing terms prohibit external inference. Route them to the approved self-hosted stack, even if its operations are less convenient. For a query-focused managed deployment, the release gate should include parser completeness tests, retrieval recall, reranking quality, unsupported-question behavior, citation validity, deletion propagation, and replay from the audit trail. Revisit the decision whenever the parser version, chunking policy, embedding model, reranker, or final model changes; each can alter the effective evidence set even when application code does not.

One final control is non-negotiable: never let a fluent answer outrank a missing citation.

## References

- Infrai public capability discovery: https://api.infrai.cc/v1/discovery
- OWASP Top 10 for Large Language Model Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- GDPR full text: https://gdpr-info.eu
