"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const rsp_1 = require("./rsp");
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, defaultGraph, quad } = DataFactory;
const EXPECTED_SINGLE_WINDOW_RESULTS = 2 + 4 + 6 + 8;
function makeQuad(id, predicate = 'http://rsp.js/test_property', object = 'http://rsp.js/test_object') {
    return quad(namedNode(`https://rsp.js/${id}`), namedNode(predicate), namedNode(object), defaultGraph());
}
function generate_data(num_events, rdfStreams) {
    for (let i = 0; i < num_events; i++) {
        rdfStreams.forEach((stream) => {
            stream.add(new Set([makeQuad(`test_subject_${i}`)]), i);
        });
    }
}
function waitForResults(emitter, expected) {
    return new Promise((resolve, reject) => {
        const results = [];
        const timeout = setTimeout(() => {
            cleanup();
            reject(new Error(`Timed out waiting for ${expected} RStream results; received ${results.length}`));
        }, 5000);
        const cleanup = () => {
            clearTimeout(timeout);
            emitter.off('RStream', onResult);
            emitter.off('error', onError);
        };
        const onResult = (result) => {
            results.push(result);
            if (results.length >= expected) {
                cleanup();
                resolve(results);
            }
        };
        const onError = (error) => {
            cleanup();
            reject(error);
        };
        emitter.on('RStream', onResult);
        emitter.on('error', onError);
    });
}
function singleWindowQuery(range = '10 STEP 2') {
    return `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE ${range}]
    WHERE{
        WINDOW :w1 { ?s ?p ?o}
    }`;
}
function twoWindowJoinQuery() {
    return `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT ?s ?o1 ?o2
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 10]
    FROM NAMED WINDOW :w2 ON STREAM :stream2 [RANGE 10 STEP 10]
    WHERE{
        WINDOW :w1 { ?s :p1 ?o1 }
        WINDOW :w2 { ?s :p2 ?o2 }
    }`;
}
test('rsp_consumer_test', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery());
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, EXPECTED_SINGLE_WINDOW_RESULTS);
    if (stream) {
        generate_data(10, [stream]);
    }
    const results = yield resultsPromise;
    expect(results).toHaveLength(EXPECTED_SINGLE_WINDOW_RESULTS);
}));
test('rsp_multiple_same_window_test', () => __awaiter(void 0, void 0, void 0, function* () {
    const query = `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
    FROM NAMED WINDOW :w2 ON STREAM :stream2 [RANGE 10 STEP 2]
    WHERE{
        WINDOW :w1 { ?s ?p ?o}
        WINDOW :w2 { ?s ?p ?o}
    }`;
    const rspEngine = new rsp_1.RSPEngine(query);
    const stream1 = rspEngine.getStream("https://rsp.js/stream1");
    const stream2 = rspEngine.getStream("https://rsp.js/stream2");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, EXPECTED_SINGLE_WINDOW_RESULTS);
    if (stream1 && stream2) {
        generate_data(10, [stream1, stream2]);
    }
    const results = yield resultsPromise;
    expect(results).toHaveLength(EXPECTED_SINGLE_WINDOW_RESULTS);
}));
test('rsp_multiple_different_window_test', () => __awaiter(void 0, void 0, void 0, function* () {
    const query = `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
    FROM NAMED WINDOW :w2 ON STREAM :stream2 [RANGE 20 STEP 10]
    WHERE{
        WINDOW :w1 { ?s ?p ?o}
        WINDOW :w2 { ?s ?p2 ?o2}
    }`;
    const rspEngine = new rsp_1.RSPEngine(query);
    const stream1 = rspEngine.getStream("https://rsp.js/stream1");
    const stream2 = rspEngine.getStream("https://rsp.js/stream2");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, EXPECTED_SINGLE_WINDOW_RESULTS);
    if (stream1 && stream2) {
        for (let i = 0; i < 10; i++) {
            stream1.add(new Set([makeQuad(`test_subject_${i}`)]), i);
            stream2.add(new Set([makeQuad(`test_subject_${i}`, 'http://rsp.js/test_property2', 'http://rsp.js/test_object2')]), i);
        }
    }
    const results = yield resultsPromise;
    expect(results).toHaveLength(EXPECTED_SINGLE_WINDOW_RESULTS);
}));
test('rsp_static_plus_window_test', () => __awaiter(void 0, void 0, void 0, function* () {
    const query = `PREFIX : <https://rsp.js/>
    REGISTER RStream <output> AS
    SELECT *
    FROM NAMED WINDOW :w1 ON STREAM :stream1 [RANGE 10 STEP 2]
    WHERE{
        ?o :hasInfo :someInfo.
        WINDOW :w1 { ?s ?p ?o}
    }`;
    const rspEngine = new rsp_1.RSPEngine(query);
    rspEngine.addStaticData(quad(namedNode('http://rsp.js/test_object'), namedNode('https://rsp.js/hasInfo'), namedNode('https://rsp.js/someInfo'), defaultGraph()));
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, EXPECTED_SINGLE_WINDOW_RESULTS);
    if (stream) {
        generate_data(10, [stream]);
    }
    const results = yield resultsPromise;
    expect(results).toHaveLength(EXPECTED_SINGLE_WINDOW_RESULTS);
}));
test('test_get_all_streams', () => {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery());
    expect(rspEngine.get_all_streams()).toEqual(["https://rsp.js/stream1"]);
});
test('normal in-order input preserves max_delay zero behavior and trailing timestamps', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery('10 STEP 10'), { max_delay: 0 });
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, 2);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('in-order-0')]), 0);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('in-order-1')]), 1);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('in-order-10')]), 10);
    const results = yield resultsPromise;
    expect(results).toHaveLength(2);
    expect(results.every((result) => result.timestamp_from === 0)).toBe(true);
    expect(results.every((result) => result.timestamp_to === 10)).toBe(true);
    expect(results.every((result) => result.logical_trigger_time === 10)).toBe(true);
    expect(results.every((result) => result.window_semantics === 'trailing')).toBe(true);
}));
test('accepts an event exactly at max_delay and delays output until the watermark closes the window', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery('10 STEP 10'), { max_delay: 5 });
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, 2);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('bounded-0')]), 0);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('bounded-10')]), 10);
    expect(rspEngine.windows[0].time).toBe(10);
    expect(rspEngine.windows[0].get_current_watermark()).toBe(5);
    const lateQuad = makeQuad('bounded-late');
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([lateQuad]), 5);
    expect(rspEngine.windows[0].time).toBe(10);
    expect(rspEngine.windows[0].get_current_watermark()).toBe(5);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('bounded-15')]), 15);
    const results = yield resultsPromise;
    expect(results).toHaveLength(2);
    expect(results.every((result) => result.timestamp_from === 0 && result.timestamp_to === 10)).toBe(true);
    expect(results.some((result) => result.bindings.toString().includes('bounded-late'))).toBe(true);
}));
test('rejects an event beyond max_delay through the public RDFStream API', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery('10 STEP 10'), { max_delay: 5 });
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, 1);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('rejected-0')]), 0);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('rejected-10')]), 10);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('too-late')]), 4);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('rejected-15')]), 15);
    const results = yield resultsPromise;
    expect(results).toHaveLength(1);
    expect(results[0].bindings.toString()).not.toContain('too-late');
}));
test('surfaces query processing errors through the engine emitter', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery('10 STEP 10'));
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const errorPromise = new Promise((resolve) => emitter.once('error', resolve));
    rspEngine.r2r.execute = () => __awaiter(void 0, void 0, void 0, function* () {
        throw new Error('synthetic query failure');
    });
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('error-0')]), 0);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('error-10')]), 10);
    yield expect(errorPromise).resolves.toEqual(new Error('synthetic query failure'));
}));
test('centered semantics expose one logical timestamp for every binding in a window', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery('10 STEP 10'), { window_semantics: 'centered' });
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, 2);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('centered-0')]), 0);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('centered-1')]), 1);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('centered-10')]), 10);
    const results = yield resultsPromise;
    expect(results).toHaveLength(2);
    expect(results.every((result) => result.timestamp_from === 0)).toBe(true);
    expect(results.every((result) => result.timestamp_to === 10)).toBe(true);
    expect(results.every((result) => result.logical_trigger_time === 5)).toBe(true);
    expect(results.every((result) => result.window_semantics === 'centered')).toBe(true);
}));
test('exact boundaries use adjacent half-open windows', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(singleWindowQuery('10 STEP 10'));
    const stream = rspEngine.getStream("https://rsp.js/stream1");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, 2);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('boundary-0')]), 0);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('boundary-10')]), 10);
    stream === null || stream === void 0 ? void 0 : stream.add(new Set([makeQuad('boundary-20')]), 20);
    const results = yield resultsPromise;
    expect(results.map((result) => [result.timestamp_from, result.timestamp_to])).toEqual([
        [0, 10],
        [10, 20],
    ]);
    expect(results[0].bindings.toString()).toContain('boundary-0');
    expect(results[0].bindings.toString()).not.toContain('boundary-10');
    expect(results[1].bindings.toString()).toContain('boundary-10');
}));
test('correlates multiple streams by logical window bounds with out-of-order input', () => __awaiter(void 0, void 0, void 0, function* () {
    const rspEngine = new rsp_1.RSPEngine(twoWindowJoinQuery(), { max_delay: 5 });
    const stream1 = rspEngine.getStream("https://rsp.js/stream1");
    const stream2 = rspEngine.getStream("https://rsp.js/stream2");
    const emitter = rspEngine.register();
    const resultsPromise = waitForResults(emitter, 2);
    const subject = 'https://rsp.js/join-subject';
    stream1 === null || stream1 === void 0 ? void 0 : stream1.add(new Set([quad(namedNode(subject), namedNode('https://rsp.js/p1'), namedNode('https://rsp.js/left'), defaultGraph())]), 0);
    stream2 === null || stream2 === void 0 ? void 0 : stream2.add(new Set([quad(namedNode(subject), namedNode('https://rsp.js/p2'), namedNode('https://rsp.js/on-time'), defaultGraph())]), 0);
    stream1 === null || stream1 === void 0 ? void 0 : stream1.add(new Set([makeQuad('join-left-10', 'https://rsp.js/p1', 'https://rsp.js/left-10')]), 10);
    stream2 === null || stream2 === void 0 ? void 0 : stream2.add(new Set([quad(namedNode(subject), namedNode('https://rsp.js/p2'), namedNode('https://rsp.js/on-time-10'), defaultGraph())]), 10);
    stream2 === null || stream2 === void 0 ? void 0 : stream2.add(new Set([quad(namedNode(subject), namedNode('https://rsp.js/p2'), namedNode('https://rsp.js/late'), defaultGraph())]), 5);
    stream1 === null || stream1 === void 0 ? void 0 : stream1.add(new Set([makeQuad('join-left-15', 'https://rsp.js/p1', 'https://rsp.js/left-15')]), 15);
    const results = yield resultsPromise;
    expect(results).toHaveLength(2);
    expect(results.every((result) => result.timestamp_from === 0 && result.timestamp_to === 10)).toBe(true);
    expect(results.some((result) => result.bindings.toString().includes('https://rsp.js/late'))).toBe(true);
    expect(results.every((result) => !result.bindings.toString().includes('https://rsp.js/on-time-10'))).toBe(true);
}));
