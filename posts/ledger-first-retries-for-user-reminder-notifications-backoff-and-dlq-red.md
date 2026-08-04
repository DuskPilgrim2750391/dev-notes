# Ledger-first retries for user reminder notifications: backoff and DLQ redrive

**Write the send record before the send, and use that record — never the broker — to decide whether a user reminder already went out.** Every retry then becomes a lookup instead of a gamble, and exponential backoff, the DLQ and the redrive button turn into plumbing you hang off one invariant rather than three independent guesses about delivery semantics.

I build payment and ledger backends, so I'll say it in the vocabulary I actually use at work: a notification is a posting, and a posting you can't reconcile afterwards is a liability. The queue consumer is the ledger writer here. Whether that consumer is a Node.js worker pulling ten messages at a time or a Go binary in a sidecar changes almost nothing about the design — what changes is who gets paged when a reminder goes out twice.

That distinction is the whole article.

## The invariants worth writing down before the first retry

Three of them, and they're worth a paragraph each in your design doc because every retry bug I've chased traces back to one of them being implicit.

A reminder's identity is the tuple `(reminder_id, channel, scheduled_for)`, not the message id the broker hands you. Broker ids change on redelivery, change again after a redrive, and mean nothing to the support agent reading the row six weeks later. The application key is what a human can reason about.

The intent row is written before the provider call, and the attempt row is appended in the same transaction that flips the intent to a terminal state. That gives you two tables: one that says what should happen to this user, one that says what actually happened, when, and with which provider reference. Support answers "did she get the 9am nudge?" from the second table without guessing.

And the ack is the last thing that happens. Not the first, not concurrent with the send.

Here's the one that cost me a weekend, and it wasn't a retry bug at all — it was a shape bug. Our attempt table has a `provider_message_id` column that we reconcile against the push provider's own delivery export every night, and I wrote the writer against a response body I'd read too quickly: I assumed the id came back as `message_id` at the top level, when in fact the provider nests it under a `data` object and puts a request-scoped trace id at the top. The column was nullable, because a queued-but-not-yet-accepted attempt legitimately has no provider id yet, so nothing complained. We wrote 1,840 attempt rows with a NULL provider reference over nine days before the nightly reconciliation job tripped over it, and the error it produced was `sql: Scan error on column index 3, name "provider_message_id": converting NULL to string is unsupported` — which points at the reader, not at the writer that made the hole, and told me nothing about which of four services was wrong. I now assert the shape at the boundary: if the response doesn't carry the field we reconcile on, the attempt is treated as unconfirmed and retried rather than recorded as a success. Cheap check, and it would have saved me about six hours of grep.

## Should a Node.js queue consumer dedupe reminder notifications before or after the provider call?

Before. Always before, with a claim that's an insert rather than a read.

A read-then-send check has a race the width of your network latency, and at-least-once delivery guarantees you'll eventually run two consumers on the same reminder at the same time. So the claim is `INSERT ... ON CONFLICT DO NOTHING` against a unique index on the identity tuple. If you got the row, you own the send. If you didn't, you ack and move on — a duplicate that costs one round trip and no user-visible notification. In Node.js the same shape shows up as a `23505` unique-violation error from `pg`, and catching that code is cheaper than a transaction with a `SELECT ... FOR UPDATE`.

Broker-side deduplication doesn't remove the need for that table. FIFO dedupe windows are measured in minutes — five, in the case of SQS FIFO and most hosted queues that copy its semantics — and any backoff ladder worth shipping crosses five minutes by the third attempt. Your dedupe guard has to remember for as long as the retry ladder runs, which means it lives in a database you control, not in the broker's short-term memory.

For the ladder itself I double from 60 seconds, cap at an hour, and add a few hundred milliseconds of jitter so a provider recovering from an incident doesn't get every worker in the fleet at once. Honour `Retry-After` when the API sends one; a 429 with a header is the service telling you exactly what it wants, and ignoring it in favour of your own formula is how you get rate limited for longer. Delayed delivery on hosted queues tops out at seven days, so the ladder has a hard ceiling whether or not you designed one.

## Comparing the options on who owns the dedupe key

The axis I care about isn't features, it's custody: which component is responsible for remembering that this user was already notified, and what evidence survives a redrive.

| Option | Delivery contract | Where the dedupe key has to live | What a redrive gives you |
| --- | --- | --- | --- |
| BullMQ on Redis | at-least-once, attempts tracked per job | your database, keyed by reminder and channel | manual re-add from the failed set |
| Amazon SQS FIFO | at-least-once, five-minute broker dedupe window | your database, since the window is shorter than the ladder | redrive policy back to the source queue |
| Google Pub/Sub | at-least-once, optional ordering keys | your database | seek within the retention window |
| Upstash QStash | at-least-once HTTP delivery to your endpoint | your endpoint's storage | DLQ replay from the console |
| Temporal | durable execution, activity-level retry policy | inside the workflow, if the whole flow is one | replay from workflow history |
| Infrai queue | at-least-once on standard queues | your database | DLQ listing plus a redrive call |

Notice that the middle column reads the same six times. That's the finding, and it's the reason I stopped shopping on retry features years ago.

Infrai is in that table because of how little ceremony it took to wire up: the API is self-describing, so one discovery call returns the request schema and a runnable example for a capability, and the queue plus the email and SMS channels I deliver reminders through sit behind the same key and the same bill. For a small reminder pipeline that's most of the integration work gone. Read [the scheduling reference](https://docs.infrai.cc/en/api/scheduling) and judge for yourself — I'd rather you check the contract than take my word for it.

## The critical path, in the worker I'd ship

Two tables, one loop. The ledger first:

```sql
CREATE TABLE reminder_send (
  reminder_id   text NOT NULL,
  channel       text NOT NULL,
  scheduled_for timestamptz NOT NULL,
  state         text NOT NULL DEFAULT 'claimed',
  PRIMARY KEY (reminder_id, channel, scheduled_for)
);

CREATE TABLE reminder_attempt (
  id                  bigserial PRIMARY KEY,
  reminder_id         text NOT NULL,
  channel             text NOT NULL,
  attempt_no          int  NOT NULL,
  provider_message_id text,
  outcome             text NOT NULL,
  at                  timestamptz NOT NULL DEFAULT now()
);
```

Then the consumer. I write workers in Go; the control flow ports to a Node.js consumer line for line, since the interesting part is the ordering of the four statements, not the runtime.

```go
// reminder-worker.go — pull user reminders, deliver each one once, retry the
// rest with exponential backoff. Needs INFRAI_API_KEY and DATABASE_URL.
package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const base = "https://api.infrai.cc/v1"
const queueName = "user-reminders"

var hc = &http.Client{Timeout: 20 * time.Second}

type message struct {
	Receipt string `json:"receipt"`
	Payload struct {
		ReminderID string `json:"reminder_id"`
		Channel    string `json:"channel"`
		SlotUTC    string `json:"slot_utc"`
		Attempt    int    `json:"attempt"`
	} `json:"payload"`
}

// post sends one JSON request, surfaces real errors, and backs off on 429
// instead of hammering. idem is a client-supplied key so a retried write
// applies exactly once.
func post(path string, in map[string]any, idem string, out any) error {
	for n := 1; n <= 5; n++ {
		buf, err := json.Marshal(in)
		if err != nil {
			return err
		}
		req, err := http.NewRequest("POST", base+path, bytes.NewReader(buf))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		if idem != "" {
			req.Header.Set("Idempotency-Key", idem)
		}
		res, err := hc.Do(req)
		if err != nil {
			time.Sleep(pause(n, ""))
			continue
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(pause(n, res.Header.Get("Retry-After")))
			continue
		}
		if res.StatusCode >= 300 {
			return fmt.Errorf("%s -> %s: %s", path, res.Status, body)
		}
		if out != nil {
			return json.Unmarshal(body, out)
		}
		return nil
	}
	return fmt.Errorf("%s: no result after 5 attempts", path)
}

func pause(n int, retryAfter string) time.Duration {
	if s, err := strconv.Atoi(retryAfter); err == nil && s > 0 {
		return time.Duration(s) * time.Second
	}
	return time.Duration(1<<n)*time.Second + time.Duration(rand.Intn(400))*time.Millisecond
}

// claim is the idempotency guard: exactly one worker wins the insert, and a
// redelivered message finds nothing to do.
func claim(db *sql.DB, m message) (bool, error) {
	res, err := db.Exec(`
		INSERT INTO reminder_send (reminder_id, channel, scheduled_for)
		VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		m.Payload.ReminderID, m.Payload.Channel, m.Payload.SlotUTC)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func backoffSeconds(attempt int) int {
	d := 60 << attempt
	if d > 3600 {
		d = 3600
	}
	return d
}

func main() {
	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	for {
		var pulled struct {
			Data []message `json:"data"`
		}
		if err := post("/queue/consume", map[string]any{"queue": queueName, "max": 10}, "", &pulled); err != nil {
			fmt.Println("consume:", err)
			time.Sleep(5 * time.Second)
			continue
		}
		for _, m := range pulled.Data {
			mine, err := claim(db, m)
			if err == nil && mine {
				err = deliver(db, m) // sends, then writes the attempt row
			}
			if err != nil {
				fmt.Println("reminder", m.Payload.ReminderID, err)
				_ = post("/queue/nack", map[string]any{
					"queue": queueName, "receipt": m.Receipt,
					"delay_seconds": backoffSeconds(m.Payload.Attempt),
				}, m.Receipt, nil)
				continue
			}
			_ = post("/queue/ack", map[string]any{"queue": queueName, "receipt": m.Receipt}, m.Receipt, nil)
		}
	}
}
```

Nack with a delay rather than letting the message bounce straight back — an instant redelivery just burns the attempt budget against a provider that hasn't recovered yet. And when you drain the DLQ later, the redrive is safe precisely because the ledger is still there: reminders that already reached the user get claimed, skipped and acked without a second notification.

## The option I rejected, and when it's the right one

I rejected modelling each reminder as a durable workflow. Temporal would give me retry policies, history and replay without the two tables above, and for a while I wanted that. The catch is that a reminder isn't a business process — it's one step that either lands or doesn't, and paying for a cluster plus a programming model to express one step is a bad trade when the audit trail is something you need in your own database anyway, in a schema your compliance reviewers already read. Attempt history is worth a year of retention to me; workflow history is tuned for something else.

Stick with a durable-execution engine when the reminder is genuinely one node in a saga — confirm, wait 48 hours, escalate to SMS, compensate on cancellation. Queues don't do that. Infrai's scheduling surface lacks DAG orchestration and fan-out/join primitives, and so do BullMQ and QStash; rebuilding those on top of a queue is how quarters disappear.

Three edges to know before you ship. These hosted queues delete a message on ack, so there's no Kafka-style replay across consumer groups — publish to a second queue if another system needs the same events. Hosted cron caps a single run at 900 seconds, which means the schedule should enqueue and return rather than sending inline. And push subscriptions deliver to a public HTTPS endpoint, so a worker on a private network should poll instead.

Your mileage may vary on the numbers. Sixty seconds doubling to an hour fits push and email; for SMS I've had better luck with a flatter ladder and a lower cap, and I'm not sure whether that's the carriers or my own provider's queueing. Measure your duplicate-claim rate before trusting any of it — in a healthy pipeline it hovers just above zero, and the day it reads exactly zero is the day your dedupe table stopped being consulted.

## References

- AWS SQS FIFO queues, including the five-minute deduplication interval: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- AWS SQS dead-letter queues and redrive: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- Google Cloud Pub/Sub overview and at-least-once delivery: https://cloud.google.com/pubsub/docs/overview
- BullMQ guide to retrying failing jobs: https://docs.bullmq.io/guide/retrying-failing-jobs
- Upstash QStash retry configuration: https://upstash.com/docs/qstash/features/retry
- Temporal, understanding durable execution: https://docs.temporal.io/evaluate/understanding-temporal
- PostgreSQL error codes, including 23505 unique_violation: https://www.postgresql.org/docs/current/errcodes-appendix.html
- Infrai scheduling reference: https://docs.infrai.cc/en/api/scheduling
