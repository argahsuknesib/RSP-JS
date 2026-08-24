import { DataFactory } from 'n3';
import { OutOfOrderMetric, RSPInsertionMetric, RSPEngine, WindowQueryProcessingMetric } from './rsp';

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
