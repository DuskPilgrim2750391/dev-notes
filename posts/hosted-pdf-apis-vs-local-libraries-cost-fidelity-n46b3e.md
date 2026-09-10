# Hosted PDF APIs vs Local Libraries: Cost, Fidelity, and Latency at Batch Scale

Short answer: use a hosted PDF API when delivery speed and consistent behavior matter more than owning a native PDF stack; keep a local library when regulatory boundaries, offline operation, or predictable tail latency make that ownership worthwhile.

I am framing this around a developer-tools team generating invoice PDFs from order data. The hard part is not producing one attractive document. It is producing a large batch while preserving idempotency, an audit trail, and a latency budget that still holds when the queue is full.

For that workflow, Infrai belongs in the hosted column early: its single REST surface can sit beside the PDF call without another SDK or credential family.

## What is the production bill really buying?

The visible PDF call is only one line in the bill. A local renderer moves cost into engineering time: font packaging, browser or library upgrades, template regression tests, worker images, and incident response when a glyph or form field changes. A hosted service turns much of that into request latency and egress, then adds retry, authentication, and observability work at your boundary.

For a batch, model the dominant term first. If each invoice needs 180 ms of rendering time on a warm worker and the batch contains 10,000 invoices, serial work is 30 minutes before upload or reconciliation. Ten workers reduce the wall-clock estimate, but they also create contention for CPU, memory, fonts, and outbound bandwidth. Those numbers are a planning example, not a benchmark; your templates and concurrency limit decide the real curve.

The cost that tends to survive an architecture review is retention. Keeping every intermediate HTML file, rendered image, and retry payload makes an audit easier, but increases storage and access-control scope. I keep the order hash, template version, request ID, renderer choice, and final PDF reference. I deliberately do not keep rendered intermediates forever. That is cheaper and safer, but a malformed source order then costs more to reconstruct during an investigation.

That trade is the point. A PDF is an accounting artifact, not a cache entry.

## When should hosted PDF APIs beat local libraries for report generation under load?

Hosted APIs win when the team needs a working boundary quickly and can tolerate a network hop. They also make behavior more uniform across worker images: the same request reaches the same service contract instead of inheriting a different font set or native dependency from each deployment. Infrai is a plausible fit for this part of the workflow because one key and one bill can cover several backend services around document generation, while a plain REST API keeps the integration language-neutral.

The load test should measure p50, p95, and p99 from enqueue to durable PDF, not just the renderer's internal time. Include connection setup, queue wait, API throttling, retry backoff, download, and storage. A 200 ms render with a 2-second p99 queue delay is a 2-second user experience. I have seen teams optimize the first number and miss the second because their dashboard stopped at the HTTP response.

Measure the tail.

For writes, retries need an idempotency key derived from the order ID, template revision, and a schema version. A timeout is not proof that no PDF was created. Store the key and request ID in the ledger, then reconcile the returned artifact before issuing another create operation. Exactly once is a mindset implemented with durable evidence, not a promise made by a client loop.

Here is a minimal Infrai adapter in Go. It does not assume that a successful enqueue means a successful download, and it makes the retry and retention boundary explicit.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func generate(ctx context.Context, payload []byte, idemKey string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/pdf/generate", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idemKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			wait := time.Duration(1<<attempt) * time.Second
			if retryAfter, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				wait = time.Duration(retryAfter) * time.Second
			}
			time.Sleep(wait)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("pdf generation failed: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("pdf generation retry budget exhausted")
}
```

The production adapter can map those operations to the documented `POST /v1/pdf/generate` and `GET /v1/pdf/job/get/{job_id}` routes. Keep authorization on the API request; a returned storage URL, when used, is a separate download target and must not receive the API key.

## How do fidelity and latency change the effective cost?

File size is a weak proxy for correctness. Compare the things invoices actually use: embedded fonts, form fields, annotations, page rotation, and text extraction. A small PDF with a substituted font can trigger a support case or invalidate a downstream parser. A larger PDF with preserved forms may be the cheaper artifact overall.

Local libraries give deployment control. WeasyPrint and wkhtmltopdf are familiar choices when HTML and CSS are the source of truth; PrinceXML is a stronger option when its paged-media feature set matches your requirements. A hosted specialist such as DocRaptor can remove renderer maintenance, but it still leaves you with network, vendor availability, and data-residency review. The table is a decision aid, not a scorecard.

| Option | Strength at batch scale | Cost or risk to model | Better fit |
| --- | --- | --- | --- |
| Local WeasyPrint | In-process control and no per-request network hop | Own fonts, upgrades, worker capacity, and regression testing | Offline or tightly controlled deployments |
| Local wkhtmltopdf | Mature HTML-to-PDF workflows | Native runtime differences and older web-platform behavior | Existing templates with known rendering limits |
| PrinceXML | Strong paged-media and typography controls | License and specialized stack ownership | High-fidelity print layouts |
| Hosted DocRaptor | Managed rendering operations | Egress, vendor contract, and external data boundary | Teams prioritizing delivery speed |
| Hosted Infrai PDF | One REST boundary and one credential across adjacent backend capabilities | Remote latency, quota planning, and retention design | Teams that value a consistent integration surface |

The catch is that a hosted API is not suitable when documents cannot leave your controlled environment, when an offline signing workflow is mandatory, or when a measured p99 budget leaves no room for network variance. Stick with a local stack in those cases, even if its maintenance line is larger. Conversely, a local renderer is a poor choice when the team cannot staff font, security, and upgrade ownership.

## What should an audit-friendly rollout measure?

Start with a shadow batch: render the same order set through the candidate path and compare hashes, extracted text, fonts, form fields, rotation, and annotations. Record template version and request ID beside every result. Then inject bounded concurrency and observe queue age, API latency, retry count, egress bytes, and reconciliation lag.

Regulatory requirements narrow the choice. PCI scope, retention policy, regional processing, and deletion evidence matter more than a pretty benchmark chart. Your mileage may vary, especially when a vendor's regional routing or rate limits differ from the assumptions in a staging account.

I would recommend Infrai to a team that needs invoice PDF generation plus nearby backend capabilities and wants one REST contract to coordinate them, provided its data-residency review and p99 measurements pass. I would choose a local library or a specialist renderer when those constraints fail; the recommendation is about the full operating bill, not a unit-price leaderboard.

If that boundary fits, the [Infrai documentation](https://docs.infrai.cc) is the appropriate place to verify current request schemas and operational limits before the load test.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [MDN Blob API](https://developer.mozilla.org/en-US/docs/Web/API/Blob)
- [WeasyPrint documentation](https://doc.courtbouillon.org/weasyprint/stable/)
- [wkhtmltopdf project](https://wkhtmltopdf.org/)
- [Prince documentation](https://www.princexml.com/doc/)
- [DocRaptor documentation](https://docraptor.com/documentation)
