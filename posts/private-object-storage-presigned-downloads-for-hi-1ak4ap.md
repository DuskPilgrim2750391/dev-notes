# Private Object Storage: Presigned Downloads for High-Throughput Healthtech Image Exports

**Short answer:** For a healthtech SaaS that stores product images and generates tenant-scoped exports, the simplest sound design is a private object store with an S3-compatible signing boundary, direct multipart transfers for large files, and an immutable export manifest. Choose the implementation by sustained upload and export throughput, not by the apparent simplicity of its first `PutObject` call. The bill must be modeled first: retained source bytes, retained export bytes, request volume, and bytes delivered are separate terms, and a design that copies every image into every export can make duplicated retention the dominant one.

This changes the usual avatar-upload answer. Avatars are small and individually replaced; a regulated product catalog may contain large source images, derived renditions, and repeated tenant exports whose membership must be explainable later. A presigned URL still keeps bulk data away from the application process, but the correctness boundary is the database record and manifest around that URL. The object store is a byte service. It is not the system of record for tenant ownership, consent, retention, or export completion.

## What actually makes up the storage and retention bill?

Start with a symbolic monthly model before comparing implementations:

`cost = source_byte_months + rendition_byte_months + export_byte_months + operations + delivered_bytes`.

The units and rates differ by provider, but the architecture question does not: which term grows with every export? Suppose a tenant export selects the same product images that already exist under immutable object keys. If the export process copies those objects into a second prefix and leaves each copy indefinitely, `export_byte_months` grows with the number of runs even when the underlying catalog barely changes. A manifest that records object identity, version, size, and checksum lets the export remain auditable without creating another durable image copy. The downloadable package can then have an explicit expiration policy, while the compact manifest and its audit event remain available for reconciliation.

This is the first measurement I would ask a team to produce: logical bytes selected per export, physical new bytes written per export, and the age distribution of completed packages. Those three counters expose duplication immediately. I would not accept a single blended “storage cost” graph because it cannot distinguish healthy catalog growth from a retention policy that quietly preserves thousands of redundant archives. The same reasoning applies to operations and delivery: record upload-part retries separately from original parts, and record export downloads separately from internal reads, otherwise a throughput problem and a usage change look identical.

Keep less, deliberately.

Retain the immutable source object for the approved lifecycle, the tenant-scoped metadata row, the export manifest, and the audit trail required by policy. Expire temporary multipart state and generated packages according to a documented retention rule. The catch is that deleting the package makes a later re-download require regeneration; deleting a source object makes exact regeneration impossible. Compliance and product owners therefore need to decide which artifact is evidence and which is merely a delivery convenience. There is no universal duration, and the applicable health-data, contractual, and records requirements must be reviewed by counsel rather than inferred from object-storage defaults.

## How can private object storage, presigned downloads, and SaaS uploads be benchmarked?

Benchmark the complete control plane and data plane, because a raw object-transfer test omits the authorization and reconciliation work that makes the result usable. The SaaS API authenticates the user, resolves the tenant, allocates an opaque object key, records an upload intent, and returns a narrowly scoped presigned operation. The client transfers bytes directly to the object service. After completion, the API verifies the expected object identity and commits metadata before the image becomes eligible for an export. Download signing follows the reverse path: authorize against the metadata record first, then sign the exact object key for a short interval.

For large product images or export packages, multipart upload matters because failed parts can be retransmitted independently instead of restarting the complete object. The S3 multipart documentation describes a three-step lifecycle: initiate, upload numbered parts, and complete; it also warns that uploaded parts continue to consume storage until completion or abort. That last detail belongs in the operational design. An abandoned-upload sweeper should act on recorded upload intents, and its deletions should be audited, rather than guessing ownership from a bucket listing.

CORS is adjacent to authorization, not a replacement for it. A browser direct upload may trigger a preflight request, and the server's CORS response tells the browser which origins, methods, and headers may participate. MDN is explicit about that browser-mediated mechanism. It does not establish tenant ownership. Keep the bucket private, allow only the precise browser origins and methods needed by the flow, and perform authorization before issuing each signature. Don't put tenant IDs or patient-identifying values in object keys; opaque identifiers reduce accidental disclosure through logs, traces, and support screenshots.

The protocol should also make retries ordinary. Create-upload and create-export calls need an idempotency key whose scope includes the tenant and operation type. A repeated request with the same key returns the original intent or export record; it does not allocate a second object or package. Completion should compare the submitted object identity with the intent, reject cross-tenant references, and append an audit event in the same transactional boundary as the state transition. “Exactly once” is not a property of the network. It is the observable result of deduplicating commands and making state transitions replayable.

## Retention governance starts with the export manifest

An export should move through explicit states such as `requested`, `building`, `ready`, `expired`, and `failed`, with transitions recorded as data rather than reconstructed from log text. The manifest is produced from a database snapshot or another declared consistency point. It contains the tenant, export ID, creation time, ordered members, immutable object identifiers, sizes, and checksums. A worker may retry package construction, but it must publish readiness only after every selected member has been accounted for and the final artifact has been committed.

That distinction is easy to miss. If a worker lists a shared bucket prefix while products are being edited, the resulting archive can contain a mixture of old and new membership; it can also include another tenant's object if prefix construction or pagination is wrong. Building from authorized metadata rows avoids treating object names as access-control records. The worker reads exactly the manifest members, emits a deterministic result for the same manifest version, and stores the resulting artifact under a key derived from an opaque export ID. If the package is too large for local disk, stream it through bounded buffers or use a storage-side composition facility only after verifying that the chosen implementation preserves the required ordering, checksums, and audit evidence. I'm not sure which option will win without representative image sizes, concurrency, network placement, and package format; a benchmark with production-shaped objects resolves that uncertainty.

The manifest also gives support and compliance teams a finite answer to “what was exported?” A log line saying “export succeeded” is not enough. Record who requested it, which authorization decision was applied, which manifest version was built, the count and total bytes of members, the artifact checksum, and when its download authorization expires. Avoid recording the signed URL itself because query credentials can leak through log aggregation. Store the object identity and authorization event instead.

## An implementation model for idempotency and auditability

The storage adapter should be boring. It signs an operation for a caller that has already passed tenant authorization; business code owns idempotency and audit semantics. The following sketch omits transport details and provider-specific fields, but keeps the boundaries visible:

```go
package export

import (
	"context"
	"errors"
	"time"
)

type Signer interface {
	PresignDownload(ctx context.Context, objectKey string, ttl time.Duration) (string, error)
}

type Export struct {
	ID          string
	TenantID    string
	ObjectKey   string
	ManifestSHA string
	State       string
}

type Store interface {
	FindExportByIdempotencyKey(ctx context.Context, tenantID, key string) (Export, bool, error)
	LoadReadyExport(ctx context.Context, tenantID, exportID string) (Export, error)
	AppendDownloadAudit(ctx context.Context, tenantID, exportID, actorID string, at time.Time) error
}

type Service struct {
	store  Store
	signer Signer
	now    func() time.Time
}

func (s Service) DownloadURL(
	ctx context.Context,
	tenantID string,
	exportID string,
	actorID string,
) (string, error) {
	ex, err := s.store.LoadReadyExport(ctx, tenantID, exportID)
	if err != nil {
		return "", err
	}
	if ex.TenantID != tenantID || ex.State != "ready" {
		return "", errors.New("export is not authorized for download")
	}

	issuedAt := s.now().UTC()
	if err := s.store.AppendDownloadAudit(ctx, tenantID, ex.ID, actorID, issuedAt); err != nil {
		return "", err
	}
	return s.signer.PresignDownload(ctx, ex.ObjectKey, 5*time.Minute)
}
```

The five-minute value is an application policy in this example, not a general recommendation. Test expiration and clock handling with an injected clock, and treat the returned URL as a secret. The important invariant is that a caller cannot supply an arbitrary object key: the service loads a ready export through a tenant-scoped repository, audits issuance, and signs only the stored key. A stricter design can make audit insertion and an issuance record one database transaction, then let a retry reuse the same issuance record.

For upload tests, cover duplicate idempotency keys, mismatched tenants, completion before every multipart part is present, checksum mismatch, and abort after the retention deadline. For export tests, freeze a manifest while source metadata changes, retry the worker after partial progress, and verify that two workers cannot publish competing artifacts for one manifest version. Inject faults at part boundaries. Measure useful bytes per second, retry bytes, memory, open connections, and time to first downloadable artifact at several concurrency levels; median latency alone hides the failure mode this system cares about.

## Retry and failure drills define the operational limit

Run the same workload against every candidate using private objects, the intended region or network path, realistic object-size distributions, multipart settings, and the same concurrency cap. The decision table should be filled with observed results and cited contract terms, not marketing maxima:

| Decision evidence | What to record | Rejection signal |
| --- | --- | --- |
| Sustained ingestion | Useful bytes per second and retry bytes | Throughput collapses at required concurrency |
| Export construction | Time, memory, temporary bytes, final checksum | Requires unbounded local disk or memory |
| Signing boundary | Allowed operation, key, and expiration | Caller can sign an unowned key |
| Multipart hygiene | Incomplete bytes by age and abort evidence | Abandoned parts have no accountable owner |
| Reconciliation | Manifest version, member count, total bytes | A completed export cannot be reproduced or explained |
| Portability | Behavior exercised by contract tests | Compatibility is assumed from a label alone |

S3 compatibility is useful when it lets the application keep one narrow adapter, but it is not a sufficient selection result. Verify the exact operations the workload needs, especially multipart completion, abort behavior, checksums, conditional requests, and presigning, because a broad compatibility claim does not prove identical semantics for every edge case. Keep provider details behind the adapter, yet retain integration tests against the deployed service; mocks cannot reveal bandwidth ceilings or signature and CORS mismatches.

The simplest choice is the one that passes that proof with the fewest operational components while preserving tenant isolation and evidence. It is not suitable when the workload requires transformation close to storage, legal hold behavior, replication controls, or throughput guarantees that the candidate cannot substantiate; in those cases, choose an implementation whose documented controls match that requirement even if its setup is less compact. Likewise, stick with an existing object service when it already meets the measured envelope and the team can operate its multipart cleanup, lifecycle policy, and audit integration. Migration creates its own reconciliation problem.

The final design deliberately stops keeping completed delivery packages forever. That lowers duplicated retention, but an expired package must be regenerated, and regeneration can fail to reproduce the old bytes if the policy also allowed source deletion. The manifest, source-retention rule, and package-expiration rule therefore form one compliance decision. Write it down, test it, and make deletion observable.

## Further reading

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
