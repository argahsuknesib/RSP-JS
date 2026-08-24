# RSP-JS evaluation instrumentation audit

## Scope and evidence

This audit compares the named branch heads present in the repository on 2026-08-24. It is source- and test-based; it does not infer benchmark results or claim a branch is correct beyond the behavior demonstrated below.

| Branch | Head | CSPARQL/window behavior | OOO and `max_delay` behavior | Query timing |
| --- | --- | --- | --- | --- |
| `main` | `7aaf44a` | Original time-driven implementation; computes and emits a selected eligible window. | No OOO configuration or classification in `RSPEngine`; no watermark-based delay policy. | None. |
| `rsp-logging` | `5bb9d34` | Pre-OOO window implementation with logging-format changes. | No evaluation-ready OOO policy. | No structured timing. |
| `ooo-support-evaluation` | `2ca9f74` | Uses a `trigger_threshold`/count-oriented trigger path that deletes the selected window when its size reaches the threshold. | Has `max_delay` and a watermark, but retains the signed late-event condition described below. | Uses `Date` around the `execute` call only; does not wait for the bindings stream to end. |
| `ooo-support` | `5dac00c` | Selects the latest eligible window, schedules emission with `setTimeout(max_delay)`, and requires `watermark >= close + max_delay` in the delayed callback. | Detects OOO with `this.time > timestamp`, but compares `timestamp - this.time` with `max_delay`; that makes every late event satisfy the "within" path. | Has `Date` calls around `execute`, but timing is not emitted and ends before asynchronous bindings consumption. |
| `ooo-support-test-fix` | `3717272` | Direct descendant of `ooo-support`; fixes half-open `getContent`, scopes relative to the first event, uses `close <= watermark`, and emits every eligible nonempty window on watermark updates. | Retains the same signed late-event condition as `ooo-support`; its latest tests cover content lookup and window-management behavior. | Same non-emitted `Date` timing around `execute`; it does not cover the asynchronous bindings completion. |

The ancestry check is decisive for the selected baseline: `ooo-support` is an ancestor of `ooo-support-test-fix`; the evaluation and logging branches are not descendants of it. The test-fix branch is therefore the only supplied branch that both contains the OOO work and carries its subsequent window-boundary/test corrections. `evaluation/4hz-instrumentation` starts from `ooo-support-test-fix` and makes the documented sign correction plus additive observability changes only.

## OOO semantic bug: established and corrected

The OOO branches classify an event as late when `this.time > timestamp` and calculate `event_latency = this.time - timestamp`. Their admission check instead used `timestamp - this.time <= max_delay`. For every genuinely late event, that expression is negative, so it is always less than or equal to a nonnegative delay; events beyond the configured bound were admitted.

The evaluation branch changes that single decision to `event_latency > max_delay` for rejection, and accepts the complementary `event_latency <= max_delay` path. This is a semantic bug fix, not an instrumentation change. Tests now demonstrate in-order input, lateness within the bound, equality at the bound, and rejection beyond the bound.

## Raw metric interface

`RSPEngine` preserves `new RSPEngine(query, options)`, `register()`, and `getStream()`. Instrumentation is additive:

```ts
const engine = new RSPEngine(query, {
  max_delay: 30000,
  metrics: { run_id, approach, client_id, query_id },
});

engine.metrics.on('rsp_insertion', observation => writeRaw(observation));
engine.metrics.on('out_of_order_event', observation => writeRaw(observation));
engine.metrics.on('window_query_processing', observation => writeRaw(observation));
engine.metrics.on('r2r_first_result', observation => writeRaw(observation));

engine.getStream(streamId)?.add(quads, eventTimeMs, eventId);
```

`onMetric(event, observation)` is also available in the options for callback-based collection. If context metadata is not supplied, each context field is the literal `"unspecified"`; an evaluator should always set stable values. When no `event_id` is supplied, `RDFStream.add` creates a stream-local sequence identifier. The optional third `add` argument is additive.

### `rsp_insertion`

```text
run_id, approach, client_id, query_id,
event_id, stream_id,
start_monotonic_ns, end_monotonic_ns, duration_ms,
event_time_ms
```

The clock starts immediately before `CSPARQLWindow.add` and ends immediately after it returns. It uses `process.hrtime.bigint()`. Network retrieval, Turtle parsing, and timestamp extraction have already completed before `RDFStream.add` is called and are therefore excluded. A logical event is the full `Set<Quad>` supplied to one `RDFStream.add` call.

### `out_of_order_event`

```text
run_id, approach, client_id, query_id,
event_id, stream_id, event_time_ms, reference_time_ms,
out_of_order, lateness_ms, max_out_of_orderness_ms, within_bound
```

Exactly one observation is emitted per `RDFStream.add` call, irrespective of the number of RDF quads in the supplied set. The reference is the window's current event-time (`this.time`) immediately before insertion, matching the branch's existing classification reference. Definitions are:

```text
out_of_order = event_time_ms < reference_time_ms
lateness_ms = reference_time_ms - event_time_ms  (only when out_of_order; otherwise 0)
max_out_of_orderness_ms = configured max_delay
within_bound = !out_of_order || lateness_ms <= max_out_of_orderness_ms
```

The maximum is not hardcoded: the observation reads the existing `max_delay` option. The target evaluation can set it to `30000`.

### `window_query_processing`

```text
run_id, approach, client_id, query_id,
window_id, window_from_ms, window_to_ms, window_size,
start_monotonic_ns, end_monotonic_ns, duration_ms
```

`window_id` is `<window name>:[<from>,<to>)`. The start is immediately before `R2ROperator.execute(data)`. The end is the Comunica bindings stream's `end` event, after binding consumption has completed. This deliberately includes query setup and asynchronous R2R result production, but excludes prior RDF event insertion and any caller-side network/parsing/timestamp work.

No summary statistics, synthetic observations, or benchmark tables are produced in this repository.

### `r2r_first_result`

```text
run_id, approach, client_id, query_id,
window_id, window_from_ms, window_to_ms, window_size,
start_monotonic_ns, end_monotonic_ns, duration_ms
```

Exactly one observation is emitted on the first `data` event from each nonempty
R2R bindings stream. The start is the same point used by
`window_query_processing`; the end is captured inside that first `data`
callback using `process.hrtime.bigint()`. Zero-binding evaluations do not emit
this observation. The existing `window_query_processing` observation remains
completion-based and unchanged.
