# Structured Output First: Cheap Embeddings and Rerank for Ask-Your-Docs Search

For a media knowledge base, the operational constraint is not the lowest advertised token rate. It is whether a search answer can return the right evidence in a schema that downstream code can validate, replay, and reconcile. **Short answer: use embeddings for recall, rerank a fixed candidate set, and compare providers on structured-output correctness, measured workload cost, and approved US/EU data handling rather than on a per-1M-token number alone.**

This is an architecture decision record for an ask-your-docs service used by editors, producers, and support staff. The corpus might contain show bibles, licensing notes, release calendars, and private production documentation. A fluent paragraph with the wrong episode identifier is a failed result. My acceptance criterion is therefore closer to a ledger invariant than to a demo score: every returned claim must be traceable to a source chunk, and every machine-readable field must survive validation.

## Decision: make retrieval evidence a typed contract

The retrieval path has two stages. An embedding search finds a bounded candidate set, and a reranker orders that set using the query and candidate text. The answer stage receives only the selected evidence and returns a schema with source IDs, quoted support, and an explicit uncertainty state. It does not silently invent a missing field.

The durable decision is the contract and its audit trail; the replaceable decision is the embedding or rerank model. Each chunk gets a stable ID from the document ID, revision, and chunker version. Each vector record stores the content hash and model ID. Each query record stores the query hash, candidate IDs, final order, and policy branch. Each metered request gets a tenant, operation ID, and internal cost center. These fields let an operator explain why `series-184:chunk-07` entered an answer after a model or chunking change.

Exactly-once delivery is not something I assume across an HTTP boundary. I persist the logical operation before dispatch, retry the same operation ID, and make the write idempotent in the storage layer. A retry after a rate limit is another attempt, not a second business event. Retrieval reads may repeat computation, but the internal usage ledger must still distinguish a logical query from its transport attempts.

For private US and EU material, the approval record must separately capture processing region, retention and deletion terms, access controls, subprocessors, and the applicable data-processing agreement. A public endpoint being reachable from Europe does not establish EU residency. A catalog page does not establish every compliance control. Those are evidence gaps to close, not assumptions to hide in a launch checklist.

Keep the boundary hard.

## How should cheap embeddings and rerank compare for semantic search?

Start with one frozen corpus and one query set. Split the cost model into initial indexing, changed-document indexing, query embeddings, candidate retrieval, reranking, and answer generation. The useful rerank driver is not the embedding rate: it is the number of queries multiplied by the bounded candidate count and the measured candidate text size, converted according to the selected API's documented billing unit. A per-1M-token figure is meaningful only when the unit, input definition, and workload are identical.

The benchmark should have labeled media questions, including title aliases, season and episode identifiers, rights terminology, and queries whose answer is absent. Record recall at the candidate stage, ranking quality after rerank, schema-validation rate, citation-support rate, latency, and metered usage. Run the same chunking policy and candidate count across alternatives. Otherwise a cheaper-looking result may just be doing less work.

I am not sure a public leaderboard can predict performance on a private archive containing house names and production shorthand. A small, reviewed judgment set from the real corpus would resolve that uncertainty. Your mileage may vary when the language mix, chunk size, or document churn changes.

| Comparison axis | What to hold constant | Failure signal | Decision use |
|---|---|---|---|
| Recall | Corpus snapshot, chunker version, query set, candidate count | The relevant chunk never enters the candidate set | Change chunking or embedding configuration before tuning rerank |
| Ranking | Candidate IDs, query text, relevance labels | Relevant evidence is present but consistently ordered below the cutoff | Test reranking selectively and record the policy branch |
| Structured output | JSON schema, validation rules, required source fields | Valid JSON contains unsupported or incomplete claims | Reject the answer and expose a typed no-answer state |
| Cost | Token definitions, request volume, rebuild rate, region, retry policy | A full re-index or retry storm breaks the approved envelope | Recalculate total workload cost; do not extrapolate from one query |
| Governance | Retention, deletion, access, region, and contract evidence | The processing path cannot be approved for the corpus | Keep the data on an eligible path or choose another integration |

This comparison deliberately does not declare a universal winner among direct model APIs. The supplied public material does not establish current model rankings or current US/EU unit prices, so publishing a precise cost-per-million claim here would be false precision. Capture the price and billing definitions at approval time, then pin the reviewed catalog and workload estimate with the decision record. Cost is one line in the ledger, not the selection rule.

## Critical path: preserve IDs from query to answer

The most important implementation detail is boring: the answer cannot cite a passage whose identifier was discarded between retrieval and generation. I pass an evidence envelope through each stage and reject output that names an unknown source or omits a required field. The example below shows the validation boundary without pretending to define a provider-specific request body.

Consider a question such as “Which territory restriction applies to the second season trailer?” The candidate set may contain an old licensing memo, a current release note, and a production brief with nearly identical language. The retriever's job is to bring the relevant chunks into the bounded set; the reranker's job is to put the current, directly relevant memo first; the answer stage's job is to cite the chunk ID and return the restriction in the agreed schema. If the current memo is absent, the validator cannot fix retrieval. If the memo is present but the generated `source_ids` names a chunk from another query, the validator must reject the result. If the answer is supported but a required `territory` field is missing, the caller should receive a typed validation failure rather than a paragraph that looks useful to a busy editor. That separation makes the failure boundary testable: recall tests exercise the first stage, ranking judgments exercise the second, and schema plus citation tests exercise the last. I don't want one aggregate “quality” number to conceal which stage lost the evidence.

Reject ambiguity early.

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Evidence struct {
	ID      string `json:"id"`
	Text    string `json:"text"`
	Ordinal int    `json:"ordinal"`
}

type Answer struct {
	Answer    string   `json:"answer"`
	SourceIDs []string `json:"source_ids"`
	Status    string   `json:"status"`
}

func validateAnswer(raw []byte, evidence map[string]Evidence) (Answer, error) {
	var answer Answer
	if err := json.Unmarshal(raw, &answer); err != nil {
		return Answer{}, fmt.Errorf("invalid JSON: %w", err)
	}
	if answer.Status != "answered" && answer.Status != "not_found" {
		return Answer{}, errors.New("status must be answered or not_found")
	}
	if answer.Status == "answered" && answer.Answer == "" {
		return Answer{}, errors.New("answered result has no answer text")
	}
	for _, id := range answer.SourceIDs {
		if _, ok := evidence[id]; !ok {
			return Answer{}, fmt.Errorf("unknown source ID %q", id)
		}
	}
	if answer.Status == "answered" && len(answer.SourceIDs) == 0 {
		return Answer{}, errors.New("answered result has no supporting source")
	}
	return answer, nil
}
```

The production worker should add the operation ID, model ID, corpus revision, and schema revision to the audit record around this function. It should also make the no-answer branch visible to callers instead of coercing it into a plausible sentence. That is the same exactly-once mindset used for a payment ledger: a failed validation must not advance the state as though an accepted answer existed.

A bounded retry policy belongs at the transport boundary. Retry only conditions that the selected API documents as transient, preserve the logical operation ID, and record attempt number and elapsed time. Do not retry a validation failure as if another network attempt could repair a malformed response. For batch indexing, checkpoint by deterministic chunk ID so a process restart resumes at known work rather than duplicating accounting events.

Record the boundary.

## What should a media team reject, and when?

Reject exhaustive reranking as the default. Sending the entire archive through a precision stage on every question couples latency and usage directly to corpus growth, while hiding whether the embedding stage failed to retrieve the answer at all. Exhaustive reranking can be reasonable for a tiny, stable collection when measured relevance justifies the extra work and the full workload remains inside an approved latency and cost envelope.

Reject an unbounded answer prompt as well. It makes citations difficult to audit and turns a retrieval miss into a generation decision. A bounded evidence envelope, a schema validator, and an explicit `not_found` state are less glamorous, but they make the failure observable.

The preferred design is not suitable when a team needs a provider-native retrieval feature, a regional control unavailable in the common integration path, or a contractual arrangement that requires a direct relationship. Stick with the direct integration when its control surface is mandatory and already governed. Use a shared runtime only when its uniform HTTP contract, credential model, and common accounting surface remove real integration work without weakening those controls.

That is the trade-off. Convenience does not override the evidence boundary.

## Operating the decision after launch

Store the corpus hash, chunker version, model identifier, schema revision, relevance judgments, and reviewed billing assumptions as one release artifact. On every model or chunking change, replay the fixed query set and compare recall, ranking, validation, citation support, latency, and usage. Keep a small adversarial set: ambiguous episode names, stale release dates, rights restrictions, and questions that should return `not_found`.

Observability should join transport attempts to logical operation IDs, but it should not log private document text by default. Metrics can expose stage-level latency, retry counts, rejected outputs, missing citations, and cost-center totals. An audit view can retain the source IDs and hashes needed for reconstruction while applying the retention policy approved for the corpus.

The launch gate is intentionally dull: representative queries clear the relevance threshold, accepted answers validate against the schema, every cited ID belongs to the retrieved evidence, the full-rebuild scenario fits the reviewed envelope, and the US/EU data path has named approval evidence. If any of those checks is missing, the system is not ready for a private archive. A polished demo is not a substitute for a reconciliable record.

## References

- [OpenAI, Function calling](https://platform.openai.com/docs/guides/function-calling)
- [ElevenLabs documentation](https://elevenlabs.io/docs)
