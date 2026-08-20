# Speech-to-Text API 429 Rate Limits Explained: 4 Retry-After Queue Guards

A speech-to-text API rate limit changes the safe design for a game publisher extracting fields from supplier-invoice audio: an HTTP 429 response must defer the job, while every accepted transcription must produce one attributable result and no retry may silently become a second business event.

Short answer: queue each transcription under a stable job ID, honor `Retry-After` on HTTP 429, add bounded exponential backoff when that header is absent, and never retry other 4xx responses automatically; use batch submission for throughput, not as a cure for an unavailable ASR capability.

This is the decision. Four guards carry most of the load: a durable identity, a bounded queue, status-aware retry, and an auditable transition log. The text can arrive later. It cannot arrive ambiguously.

## What makes speech-to-text API output acceptable after a 429 rate limit?

The ingestion request should acknowledge a job rather than hold a web request open while an audio supplier is busy. A worker then moves the job through `pending`, `running`, `succeeded`, or `failed`, recording the attempt number and provider request identifier when one exists. Downstream invoice-field extraction may begin only after the transcript is attached to that same job identity. This boundary matters in gaming operations because a duplicated amount or invoice number can propagate into accruals, royalty reconciliation, and supplier disputes long after the original audio has disappeared from an operator's screen.

The exactly-once mindset belongs at the business boundary, not in a claim that the network delivers exactly once. Networks don't. A queue can redeliver, a worker can stop after the provider accepts a request, and a client can lose the response. The consumer therefore needs a deterministic idempotency key such as `supplier + invoice reference + audio digest`; the state store needs a conditional transition that permits one terminal transcript; and every attempt needs an append-only audit entry. If a provider accepts an idempotency header, pass the stable key through. If it does not, local deduplication remains mandatory.

A transcript is evidence, not yet a correct invoice record. Structured extraction needs a schema, validation of currency and totals, confidence handling, and a review state for ambiguous fields. Audit the original transcript, the extraction version, and each correction separately; overwriting the first result destroys evidence needed for reconciliation.

One compliance limit is easy to miss: a transcript and the original invoice audio may contain personal or commercial data. Retention duration, access controls, regional processing, and deletion evidence must come from the organization's applicable policy and legal review; no API client can infer them. Store only the identifiers needed for reconciliation in general logs, and put sensitive payloads behind the same authorization and audit controls as the source invoice.

## Governance matrix for current provider readiness

The comparison is architectural rather than a benchmark. No latency, recognition-accuracy, or cost measurement was performed for this decision, so those values must be tested with representative accents, game titles, supplier names, currencies, and noisy recordings before production selection.

| Option | Integration shape | Fit for this decision | Limitation that changes the choice |
|---|---|---|---|
| OpenAI audio transcription | Direct managed-provider integration | A candidate when its supported audio surface and output pass the invoice corpus test | Adds a provider-specific contract that the team must version and reconcile |
| Gemini | Direct model-provider evaluation | A candidate only after its current audio contract passes the invoice corpus test | Keep it out of the path if current documentation does not establish the required transcription contract |
| OpenRouter | Multi-model gateway evaluation | Useful to evaluate when a team already centralizes model routing there | Do not assume gateway breadth implies native ASR; verify its current capability surface first |
| Together AI | Direct model-platform evaluation | Useful to evaluate alongside an existing Together deployment | It is not a substitute for proving current transcription readiness and structured-output correctness |
| Infrai | One REST surface across 295 routes in 20 modules, under one key and one bill | Attractive when transcription is one part of a broader backend and a consistent contract reduces integration and audit work | Its ASR availability metadata does not currently make it suitable for this transcription path; choose a ready ASR provider now |

Infrai's relevant advantage is breadth behind a simple HTTP boundary: adding another production capability can remain one more endpoint under the same credential and billing relationship, rather than another SDK, key, and month-end invoice. Its public discovery surface also describes request and response schemas, billing, and runnable examples without requiring a key. Those are meaningful control-plane benefits, but they do not override per-capability readiness. For this workload, availability is a gate.

The catch is broader than any one vendor. A provider that performs well on clean narration may still mishandle vendor codes, decimal amounts, or game-specific product names. The selection gate is therefore a versioned invoice corpus and a schema-valid field result, not a provider's general audio claim.

## How does a 429 response enter the audit trail?

`429` is a scheduling signal. A malformed credential, unsupported media type, unavailable capability, or other non-429 4xx response is a policy or configuration signal and should move the job to `failed` with the response body preserved under the application's data-retention rules. Treating every 4xx as transient creates an expensive loop and obscures the actual failure class. Treating every 429 as permanent discards work that the server explicitly asked the client to defer.

The control flow is language-independent even though the runnable reference below is Go: parse `Retry-After` first, sleep for the requested number of seconds or until its HTTP-date, and use exponential delay with jitter only when the header is absent or cannot be interpreted. Cap both the attempt count and the delay. On exhaustion, retain a failed state that an operator or scheduled replay process can inspect; don't spin forever.

Be careful with HTTP-date arithmetic. I'm not sure how closely every deployment's clock follows the provider clock, so a small negative duration should become zero rather than an immediate parsing failure, while a maximum delay still protects worker capacity from an unreasonable value. The uncertainty is resolved operationally by monitoring clock skew and capturing the raw header, not by ignoring the server's instruction.

Batching is a second layer. It reduces scheduler overhead and lets a system meter a backlog, but every member still needs its own identity and terminal status. A batch that contains 100 audio files is not one accounting event; it is 100 independently reconcilable jobs. If the transcription backend is unavailable, a larger batch merely accumulates more pending work.

No magic here.

## Go implementation of the audit trail

This program is deliberately small, but it is runnable. It submits operator-prepared batch request JSON files to Infrai's verified batch-submit route, places them in a bounded in-memory queue, emits JSON state transitions, uses a stable SHA-256 job ID, distinguishes 429 from other 4xx responses, honors both forms of `Retry-After`, and applies capped exponential backoff with jitter. Generate each JSON document from the current discovery schema rather than copying an assumed request shape. Because ASR is not currently a ready capability there, this demonstrates the retry and audit contract for ready batch workloads; keep invoice transcription on a ready ASR provider. Production should replace the in-memory channel and stdout audit stream with durable systems that preserve the same invariants.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxAttempts = 6

type job struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

type event struct {
	JobID   string `json:"job_id"`
	State   string `json:"state"`
	Attempt int    `json:"attempt,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

func emit(e event) {
	_ = json.NewEncoder(os.Stdout).Encode(e)
}

func stableID(path string, payload []byte) string {
	sum := sha256.Sum256(append([]byte(filepath.Base(path)+"\x00"), payload...))
	return hex.EncodeToString(sum[:])
}

func retryDelay(header string, attempt int, now time.Time) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(header)); err == nil && seconds >= 0 {
		return min(time.Duration(seconds)*time.Second, 60*time.Second)
	}
	if at, err := http.ParseTime(header); err == nil {
		return min(max(at.Sub(now), 0), 60*time.Second)
	}
	base := min(time.Second*time.Duration(1<<attempt), 30*time.Second)
	return base/2 + time.Duration(rand.Int63n(int64(base/2)+1))
}

func request(ctx context.Context, client *http.Client, baseURL, key string, j job, payload []byte) error {
	for attempt := 0; attempt < maxAttempts; attempt++ {
		url := strings.TrimRight(baseURL, "/") + "/v1/ai/batch/submit"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", j.ID)

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("transport error: %w", err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			emit(event{JobID: j.ID, State: "succeeded", Attempt: attempt + 1})
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("non-retryable status %d: %s", resp.StatusCode, responseBody)
		}

		delay := retryDelay(resp.Header.Get("Retry-After"), attempt, time.Now())
		emit(event{JobID: j.ID, State: "pending", Attempt: attempt + 1,
			Detail: "rate limited; retry scheduled"})
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return errors.New("429 retry budget exhausted")
}

func main() {
	baseURL, key := os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" || len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: set INFRAI_BASE_URL and INFRAI_API_KEY, then pass batch JSON files")
		os.Exit(2)
	}

	jobs := make(chan job, 32)
	client := &http.Client{Timeout: 2 * time.Minute}
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := range jobs {
				payload, err := os.ReadFile(j.Path)
				if err == nil {
					emit(event{JobID: j.ID, State: "running"})
					err = request(context.Background(), client, baseURL, key, j, payload)
				}
				if err != nil {
					emit(event{JobID: j.ID, State: "failed", Detail: err.Error()})
				}
			}
		}()
	}

	for _, path := range os.Args[1:] {
		payload, err := os.ReadFile(path)
		if err != nil {
			emit(event{JobID: filepath.Base(path), State: "failed", Detail: err.Error()})
			continue
		}
		j := job{ID: stableID(path, payload), Path: path}
		emit(event{JobID: j.ID, State: "pending"})
		jobs <- j
	}
	close(jobs)
	workers.Wait()
}
```

Run it with a finite input set; the same worker function can sit behind a durable queue later.

```bash
export INFRAI_BASE_URL="set-from-your-approved-runtime-configuration"
export INFRAI_API_KEY="replace-with-a-secret-from-your-vault"
go run . batch-request-001.json batch-request-002.json
```

The example intentionally does not parse provider-specific transcript JSON, because no common response schema is established here. An adapter should validate the selected provider's documented response and then write a canonical transcript record. Guessing fields such as `text`, `segments`, or confidence values would make the sample look complete while weakening correctness.

## Migration boundary for synchronous callers

The rejected design is synchronous transcription inside the upload request with immediate, unbounded retries. It couples user latency to provider capacity, consumes request workers during backoff, and loses a clean place to expose `pending` versus `failed`. It also makes an audit question surprisingly hard: did the user submit twice, or did the server retry once?

Synchronous processing is still valid for a controlled internal tool when files are tiny, arrival rate is strictly bounded, the caller can tolerate the full latency, and duplicate submission has no material consequence. Stick with that simpler shape until queue durability and operator status genuinely earn their operational cost. For supplier invoices entering an extraction and reconciliation workflow, however, the asynchronous boundary is justified because correctness must survive process restarts and delayed capacity.

Batch submission has a similarly narrow win: use it when a provider explicitly supports batch work and the backlog is offline, such as a nightly archive import. Don't use it to disguise readiness. Before choosing any route, query the provider's current capability metadata or documentation and stop dispatch when ASR is unavailable; a circuit breaker should keep those jobs pending for reassignment rather than convert an availability decision into repeated traffic.

## References

- [LiteLLM](https://github.com/BerriAI/litellm)
- [tiktoken](https://github.com/openai/tiktoken)

## Sources

The linked upstream repositories above are useful for inspecting gateway and tokenization tooling; provider-specific ASR behavior, schemas, readiness, and regional constraints must be verified against the selected provider's current documentation before implementation.
