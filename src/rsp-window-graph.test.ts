import { Readable } from 'stream';
import { RDFStream, RSPEngine } from './rsp';
import { CSPARQLWindow, QuadContainer, ReportStrategy, Tick } from './operators/s2r';

const { DataFactory } = require('n3');
const { defaultGraph, namedNode, quad } = DataFactory;

function defaultGraphQuad(subject: string, predicate = 'https://rsp.js/p', object = 'https://rsp.js/o') {
    return quad(namedNode(subject), namedNode(predicate), namedNode(object), defaultGraph());
}

const singleWindowQuery = `PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT *
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 10]
WHERE {
    WINDOW :w1 { ?s ?p ?o }
}`;

test('assigns the window NamedNode to DefaultGraph quads in logical events', () => {
    const window = new CSPARQLWindow('https://rsp.js/w1', 10, 10, ReportStrategy.OnWindowClose, Tick.TimeDriven, 0, 0);
    const stream = new RDFStream('https://rsp.js/stream1', window);
    let emitted: QuadContainer | undefined;
    window.subscribe('RStream', data => { emitted = data; });

    stream.add(new Set([
        defaultGraphQuad('https://rsp.js/set-subject-1'),
        defaultGraphQuad('https://rsp.js/set-subject-2'),
    ]), 1);
    stream.add(defaultGraphQuad('https://rsp.js/individual-subject'), 11);

    expect(emitted).toBeDefined();
    const quads = [...emitted!.elements];
    expect(quads).toHaveLength(2);
    expect(quads.every(q => q.graph.termType === 'NamedNode')).toBe(true);
    expect(quads.every(q => q.graph.value === 'https://rsp.js/w1')).toBe(true);
    expect(quads.every(q => q.graph.value !== undefined)).toBe(true);
});

test('a DefaultGraph single-window event produces an RStream binding', async () => {
    const engine = new RSPEngine(singleWindowQuery);
    const stream = engine.getStream('https://rsp.js/stream1');
    const output = engine.register();
    const result = new Promise<boolean>(resolve => output.once('RStream', () => resolve(true)));

    stream!.add(defaultGraphQuad('https://rsp.js/single-window'), 1);
    stream!.add(defaultGraphQuad('https://rsp.js/trigger'), 11);

    await expect(Promise.race([
        result,
        new Promise<boolean>(resolve => setTimeout(() => resolve(false), 1000)),
    ])).resolves.toBe(true);
});

const threeWindowQuery = `PREFIX : <https://rsp.js/>
REGISTER RStream <output> AS
SELECT *
FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 10]
FROM NAMED WINDOW :w2 ON STREAM :stream2 [RANGE 10 STEP 10]
FROM NAMED WINDOW :w3 ON STREAM :stream3 [RANGE 10 STEP 10]
WHERE {
    WINDOW :w1 { ?s :p1 ?o1 }
    WINDOW :w2 { ?s :p2 ?o2 }
    WINDOW :w3 { ?s :p3 ?o3 }
}`;

test('combining three windows retains all distinct named graph identities', async () => {
    const engine = new RSPEngine(threeWindowQuery);
    const executed: QuadContainer[] = [];
    (engine as any).r2r.execute = jest.fn(async (container: QuadContainer) => {
        executed.push(container);
        return Readable.from([]);
    });
    engine.register();

    const subject = 'https://rsp.js/shared-subject';
    for (const [streamName, predicate] of [
        ['https://rsp.js/stream1', 'https://rsp.js/p1'],
        ['https://rsp.js/stream2', 'https://rsp.js/p2'],
        ['https://rsp.js/stream3', 'https://rsp.js/p3'],
    ]) {
        engine.getStream(streamName)!.add(new Set([defaultGraphQuad(subject, predicate)]), 1);
    }
    for (const streamName of ['https://rsp.js/stream1', 'https://rsp.js/stream2', 'https://rsp.js/stream3']) {
        engine.getStream(streamName)!.add(new Set([defaultGraphQuad(subject)]), 11);
    }

    await new Promise(resolve => setImmediate(resolve));
    expect(executed.length).toBeGreaterThan(0);
    const graphValues = new Set([...executed[0].elements].map(q => q.graph.value));
    expect(graphValues).toEqual(new Set([
        'https://rsp.js/w1',
        'https://rsp.js/w2',
        'https://rsp.js/w3',
    ]));
    expect([...graphValues].every(value => value !== undefined)).toBe(true);
});

test('a query requiring all three WINDOW/GRAPH clauses produces a binding', async () => {
    const engine = new RSPEngine(threeWindowQuery);
    const output = engine.register();
    const result = new Promise<boolean>(resolve => output.once('RStream', () => resolve(true)));
    const subject = 'https://rsp.js/join-subject';

    engine.getStream('https://rsp.js/stream1')!.add(defaultGraphQuad(subject, 'https://rsp.js/p1'), 1);
    engine.getStream('https://rsp.js/stream2')!.add(defaultGraphQuad(subject, 'https://rsp.js/p2'), 1);
    engine.getStream('https://rsp.js/stream3')!.add(defaultGraphQuad(subject, 'https://rsp.js/p3'), 1);
    engine.getStream('https://rsp.js/stream1')!.add(defaultGraphQuad(subject, 'https://rsp.js/p1'), 11);
    engine.getStream('https://rsp.js/stream2')!.add(defaultGraphQuad(subject, 'https://rsp.js/p2'), 11);
    engine.getStream('https://rsp.js/stream3')!.add(defaultGraphQuad(subject, 'https://rsp.js/p3'), 11);

    await expect(Promise.race([
        result,
        new Promise<boolean>(resolve => setTimeout(() => resolve(false), 1000)),
    ])).resolves.toBe(true);
});

test('window graph assignment does not change out-of-order classification', () => {
    const engine = new RSPEngine(singleWindowQuery, { max_delay: 10 });
    const observations: any[] = [];
    engine.metrics.on('out_of_order_event', metric => observations.push(metric));
    const stream = engine.getStream('https://rsp.js/stream1')!;
    stream.add(defaultGraphQuad('https://rsp.js/in-order'), 10, 'in-order');
    stream.add(defaultGraphQuad('https://rsp.js/late'), 5, 'late');

    expect(observations).toHaveLength(2);
    expect(observations[1]).toMatchObject({ out_of_order: true, lateness_ms: 5, within_bound: true });
});
