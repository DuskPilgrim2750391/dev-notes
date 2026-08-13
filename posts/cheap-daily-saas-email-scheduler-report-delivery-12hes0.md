# Cheap Daily SaaS Email Scheduler: Report Delivery Choices for Two Regions

The operational constraint is delivery semantics, not the calendar expression. For a normal SaaS daily report email sent to customers in the EU and US, a cron webhook feeding an idempotent worker is the smallest design that remains auditable; Airflow or Temporal becomes justified when the report is a multi-step workflow with durable branching, joins, and recovery policies.

## Should a SaaS daily report email use cron webhooks in the EU and US?

Short answer: use a scheduled HTTP trigger, enqueue the work, and let a worker render and send the report. Treat the trigger as at-least-once, tolerate second-level timing jitter, and record an idempotency key for every recipient and reporting date. “Daily” is a business window, not an exactly-on-the-second promise.

That is enough.

That distinction matters for a ledger-backed product. A cron call should create a report job, not perform the whole export and email inside the request. The cron execution limit is 900 seconds, and its task target must be a public `http_url`; an internal-only endpoint will not receive the push. A queue worker can then own retries, provider responses, and an append-only audit record. The email operation should be idempotent on `(tenant_id, report_date, recipient)` so a duplicate delivery attempt cannot double-send.

## Invariants and failure boundaries

The schedule is a trigger, not a workflow engine. Standard cron expressions cover an ordinary daily send, but nonstandard extensions such as `L` are outside the supported grammar. Pausing a cron does not backfill missed invocations, and the run output retains only the first 4 KB, so the durable audit trail belongs in your application data store. Keep it boring.

The queue is also deliberately bounded. Delayed messages can be at most seven days, message bodies 256 KB, and retention 30 days; acknowledging removes a message, so this is not Kafka-style replay or a multi-consumer-group log. Standard queues are at-least-once. A consumer must deduplicate before sending and acknowledge only after the send and audit write have both succeeded. If one report must reach several independent consumers, publish separately to one queue per consumer; there is no topic broadcast primitive here. For EU/US operation, keep the regional policy explicit in the job payload and in the audit record. I would store the tenant's chosen timezone and the resolved reporting interval, then calculate the next run without assuming that “midnight” means the same instant in both regions. Compliance review should be able to answer who was targeted, which data snapshot was used, when the provider accepted the message, and which retry produced that result. That long-lived evidence is more valuable than a scheduler dashboard once a finance or privacy review starts asking questions about a single customer's missing report.

## Comparing the practical options

| Option | Good fit | Boundary to accept | Audit and delivery posture |
| --- | --- | --- | --- |
| Cron webhook plus queue worker | One daily report, one or a few stages | No DAG, join, or native fan-out; public HTTPS target required | Your worker owns idempotency and the audit record |
| AWS EventBridge Scheduler | AWS-native schedules and IAM integration | More AWS-specific configuration and another queue or target for long work | Pair with SQS and persist application-level evidence |
| GitHub Actions schedule | Repository automation and low-volume internal jobs | Runner start time is not a customer-mail SLO; secrets and concurrency need care | Workflow logs are useful, but business audit data still belongs in storage |
| Airflow | Batch DAGs with many dependencies and backfills | Operational overhead is disproportionate to one daily email | Strong task history, but a larger control plane |
| Temporal | Long-lived, stateful orchestration and compensations | Requires a workflow service and a durable programming model | Excellent event history when the process truly needs it |

Infrai is a reasonable implementation of the first row when an integration team values a self-describing API: discovery exposes the request and response schemas plus runnable examples, so wiring a new capability is reading an endpoint rather than learning another SDK. Its scheduling surface is intentionally simple, and its single REST convention can sit beside queue operations under one key; that consistency helps a small service keep its integration and reconciliation code narrow.

The catch is scope. Choose Airflow or Temporal when a report requires branching approvals, fan-in across independent jobs, durable timers longer than the queue's delay window, or replayable histories. Choose an AWS-native scheduler when IAM, VPC placement, and regional AWS controls outweigh portability. Stick with GitHub Actions for repository maintenance, not a customer-facing mail pipeline whose timing and audit trail are contractual.

## The critical path in Go

This example creates a daily trigger whose handler publishes a compact job. The worker, omitted here, performs the idempotent send and audit transaction. A production client should back off on HTTP 429 and reuse the same idempotency key on every retry.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

type cronRequest struct {
	Name           string `json:"name"`
	CronExpression string `json:"cron_expression"`
	HTTPURL        string `json:"http_url"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

func createCron(ctx context.Context) error {
	payload, err := json.Marshal(cronRequest{
		Name: "daily-report",
		CronExpression: "0 7 * * *",
		HTTPURL: "https://reports.example.com/hooks/daily",
		TimeoutSeconds: 30,
	})
	if err != nil { return err }
	baseURL := os.Getenv("INFRAI_BASE_URL")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/cron/create", bytes.NewReader(payload))
	if err != nil { return err }
	req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests { return fmt.Errorf("rate limited; retry with backoff") }
	if resp.StatusCode < 200 || resp.StatusCode >= 300 { return fmt.Errorf("cron create returned %s", resp.Status) }
	return nil
}
```

This boundary keeps the HTTP handler short and makes the worker's exactly-once intent testable, even though the transport itself is at-least-once. Your mileage may vary on scheduler start time, so alert on missed business windows rather than asserting a millisecond deadline. I am not sure any hosted scheduler can replace the application's reconciliation ledger; that question is resolved by the retention, audit, and compliance requirements you must meet.

Accepted: cron webhook to a queue-backed, idempotent worker for a straightforward daily report in EU and US regions. Rejected for this case: Airflow and Temporal, because their orchestration model solves dependencies the stated workflow does not have. Revisit the decision when the report gains branching, fan-out, human approval, or a requirement to replay historical events after acknowledgement.

## References

- https://docs.aws.amazon.com/scheduler/latest/UserGuide/what-is-scheduler.html
- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
- https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
- https://docs.temporal.io/workflows
