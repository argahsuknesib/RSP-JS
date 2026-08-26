import { DataFactory } from 'n3';
import { OutOfOrderMetric, R2RFirstResultMetric, RSPInsertionMetric, RSPEngine, WindowQueryProcessingMetric } from './rsp';

const { namedNode, defaultGraph, quad } = DataFactory;

const query = `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 10]
    WHERE {
        WINDOW :w1 { ?s ?p ?o }
    }`;

function event(id: string) {
    return new Set([quad(
        namedNode(`https://rsp.js/event/${id}`),
        namedNode('https://rsp.js/p'),
        namedNode('https://rsp.js/o'),
        defaultGraph(),
    )]);
}

test('emits one insertion and one OOO observation per logical multi-quad event', () => {
    const engine = new RSPEngine(query, {
        max_delay: 10,
        metrics: { run_id: 'run-1', approach: 'rsp', client_id: 'client-1', query_id: 'query-1' },
    });
    const insertions: RSPInsertionMetric[] = [];
    const ooo: OutOfOrderMetric[] = [];
    engine.metrics.on('rsp_insertion', metric => insertions.push(metric));
    engine.metrics.on('out_of_order_event', metric => ooo.push(metric));

    const stream = engine.getStream('https://rsp.js/stream1');
    expect(stream).toBeDefined();
    const multiQuadEvent = new Set([...event('one'), ...event('two')]);
    stream?.add(multiQuadEvent, 100, 'event-100');

    expect(insertions).toHaveLength(1);
    expect(ooo).toHaveLength(1);
    expect(insertions[0]).toMatchObject({ event_id: 'event-100', stream_id: 'https://rsp.js/stream1', event_time_ms: 100 });
    expect(ooo[0]).toMatchObject({ event_id: 'event-100', out_of_order: false, lateness_ms: 0, max_out_of_orderness_ms: 10, within_bound: true });
    expect(BigInt(insertions[0].end_monotonic_ns) >= BigInt(insertions[0].start_monotonic_ns)).toBe(true);
    expect(insertions[0].duration_ms).toBeGreaterThanOrEqual(0);
});

test('emits window query timing only after the binding stream completes', async () => {
    const engine = new RSPEngine(query, {
        metrics: { run_id: 'run-2', approach: 'rsp', client_id: 'client-2', query_id: 'query-2' },
    });
    const metrics: WindowQueryProcessingMetric[] = [];
    const completion = new Promise<void>((resolve) => {
        engine.metrics.on('window_query_processing', metric => {
            metrics.push(metric);
            resolve();
        });
    });
    const stream = engine.getStream('https://rsp.js/stream1');
    engine.register();
    stream?.add(event('first'), 1, 'event-1');
    stream?.add(event('second'), 11, 'event-11');

    await completion;

    expect(metrics).toHaveLength(1);
    expect(metrics[0]).toMatchObject({
        run_id: 'run-2',
        approach: 'rsp',
        client_id: 'client-2',
        query_id: 'query-2',
        window_from_ms: 1,
        window_to_ms: 11,
    });
    expect(BigInt(metrics[0].end_monotonic_ns) >= BigInt(metrics[0].start_monotonic_ns)).toBe(true);
    expect(metrics[0].duration_ms).toBeGreaterThanOrEqual(0);
});

test('emits first R2R result timing once for a multi-binding window evaluation', async () => {
    const engine = new RSPEngine(query, {
        metrics: { run_id: 'run-first', approach: 'rsp', client_id: 'client-first', query_id: 'query-first' },
    });
    const firstResults: R2RFirstResultMetric[] = [];
    const outputBindings: any[] = [];
    const completion = new Promise<void>((resolve) => {
        engine.metrics.on('r2r_first_result', metric => firstResults.push(metric));
        engine.metrics.on('window_query_processing', () => resolve());
    });
    const output = engine.register();
    output.on('RStream', (binding: any) => outputBindings.push(binding));
    const stream = engine.getStream('https://rsp.js/stream1');
    stream?.add(new Set([...event('one'), ...event('two')]), 1, 'event-1');
    stream?.add(event('trigger'), 11, 'event-11');

    await completion;

    expect(outputBindings).toHaveLength(2);
    expect(firstResults).toHaveLength(1);
    expect(firstResults[0]).toMatchObject({
        run_id: 'run-first',
        approach: 'rsp',
        client_id: 'client-first',
        query_id: 'query-first',
        window_from_ms: 1,
        window_to_ms: 11,
        window_size: 2,
    });
    expect(BigInt(firstResults[0].end_monotonic_ns) >= BigInt(firstResults[0].start_monotonic_ns)).toBe(true);
    expect(firstResults[0].duration_ms).toBeGreaterThanOrEqual(0);
});

test('does not emit first R2R result timing for a zero-result window evaluation', async () => {
    const noMatchQuery = `PREFIX : <https://rsp.js/>
        REGISTER RStream <output> AS
        SELECT *
        FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 10]
        WHERE {
            WINDOW :w1 { ?s :doesNotMatch ?o }
        }`;
    const engine = new RSPEngine(noMatchQuery, {
        metrics: { run_id: 'run-zero', approach: 'rsp', client_id: 'client-zero', query_id: 'query-zero' },
    });
    const firstResults: R2RFirstResultMetric[] = [];
    const completion = new Promise<void>((resolve) => {
        engine.metrics.on('r2r_first_result', metric => firstResults.push(metric));
        engine.metrics.on('window_query_processing', () => resolve());
    });
    const stream = engine.getStream('https://rsp.js/stream1');
    engine.register();
    stream?.add(event('first'), 1, 'event-1');
    stream?.add(event('second'), 11, 'event-11');

    await completion;

    expect(firstResults).toHaveLength(0);
});

test('emits structured metrics when diagnostic logging is disabled', async () => {
    const previous = process.env.RSP_JS_DISABLE_LOGGING;
    process.env.RSP_JS_DISABLE_LOGGING = '1';
    try {
        const engine = new RSPEngine(query, { metrics: { run_id: 'run-disabled', approach: 'rsp', client_id: 'client-disabled', query_id: 'query-disabled' } });
        const seen = new Set<string>();
        const completion = new Promise<void>((resolve) => engine.metrics.on('window_query_processing', () => resolve()));
        for (const name of ['rsp_insertion', 'out_of_order_event', 'window_query_processing', 'r2r_first_result']) engine.metrics.on(name, () => seen.add(name));
        const stream = engine.getStream('https://rsp.js/stream1');
        engine.register();
        stream?.add(event('disabled-first'), 1, 'disabled-first');
        stream?.add(event('disabled-trigger'), 11, 'disabled-trigger');
        await completion;
        expect(seen).toEqual(new Set(['rsp_insertion', 'out_of_order_event', 'window_query_processing', 'r2r_first_result']));
    } finally {
        if (previous === undefined) delete process.env.RSP_JS_DISABLE_LOGGING;
        else process.env.RSP_JS_DISABLE_LOGGING = previous;
    }
});
