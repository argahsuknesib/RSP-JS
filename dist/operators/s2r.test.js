"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const s2r_1 = require("./s2r");
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;
function generate_data(num_events, csparqlWindow) {
    for (let i = 0; i < num_events; i++) {
        const stream_element = quad(namedNode('https://rsp.js/test_subject_' + i), namedNode('http://rsp.js/test_property'), namedNode('http://rsp.js/test_object'), defaultGraph());
        csparqlWindow.add(stream_element, i);
    }
}
test('create_graph_container', () => {
    const quad1 = quad(namedNode('https://ruben.verborgh.org/profile/#me'), namedNode('http://xmlns.com/foaf/0.1/givenName'), literal('Ruben', 'en'), defaultGraph());
    const quad2 = quad(namedNode('https://ruben.verborgh.org/profile/#me'), namedNode('http://xmlns.com/foaf/0.1/lastName'), literal('Verborgh', 'en'), defaultGraph());
    let content = new Set;
    content.add(quad1);
    content.add(quad2);
    let container = new s2r_1.QuadContainer(content, 0);
    expect(container.len()).toBe(2);
    expect(container.last_time_changed()).toBe(0);
});
test('add_to_window', () => {
    const quad1 = quad(namedNode('https://rsp.js/test_subject_0'), namedNode('http://rsp.js/test_property'), namedNode('http://rsp.js/test_object'), defaultGraph());
    const quad2 = quad(namedNode('https://rsp.js/test_subject_1'), namedNode('http://rsp.js/test_property'), namedNode('http://rsp.js/test_object'), defaultGraph());
    let csparqlWindow = new s2r_1.CSPARQLWindow(":window1", 10, 2, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    csparqlWindow.add(quad, 0);
});
test('test_scope', () => {
    let csparqlWindow = new s2r_1.CSPARQLWindow(":window1", 10, 2, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    csparqlWindow.scope(4);
    let num_active_windows = csparqlWindow.active_windows.size;
    /**
     * open windows:
     * [-6, 4)
     * [-4, 6)
     * [-2, 8)
     * [0, 10)
     * [2, 12)
     * [4, 14)
     */
    expect(num_active_windows).toBe(6);
});
test('test_evictions', () => {
    let csparqlWindow = new s2r_1.CSPARQLWindow(":window1", 10, 2, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    generate_data(10, csparqlWindow);
    expect(csparqlWindow.active_windows.size).toBe(5);
});
test('test_stream_consumer', () => {
    let recevied_data = new Array();
    let received_elementes = new Array;
    let csparqlWindow = new s2r_1.CSPARQLWindow(":window1", 10, 2, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    // register window consumer
    csparqlWindow.subscribe('RStream', function (data) {
        recevied_data.push(data);
        data.elements.forEach(item => received_elementes.push(item));
    });
    // generate some data
    generate_data(10, csparqlWindow);
    expect(recevied_data.length).toBe(4);
    expect(received_elementes.length).toBe(2 + 4 + 6 + 8);
});
test('test_content_get', () => {
    let recevied_data = new Array();
    let received_elementes = new Array;
    let csparqlWindow = new s2r_1.CSPARQLWindow(":window1", 10, 2, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    // generate some data
    generate_data(10, csparqlWindow);
    let content = csparqlWindow.getContent(10);
    expect(content).toBeDefined();
    if (content) {
        // Window bounds are half-open, so timestamp 10 belongs to [2, 12).
        expect(content.elements.size).toBe(8);
    }
    let undefinedContent = csparqlWindow.getContent(20);
    expect(undefinedContent).toBeUndefined();
});
test('scope aligns windows using floor rounding without duplicate instances', () => {
    const window = new s2r_1.CSPARQLWindow(':window1', 10, 2, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    window.scope(4);
    expect(window.active_windows.size).toBe(6);
    expect((0, s2r_1.computeWindowIfAbsent)(window.active_windows, new s2r_1.WindowInstance(-6, 4), () => new s2r_1.QuadContainer(new Set(), 0))).toBe(true);
});
test('classifies and accepts a late event exactly at max_delay without regressing time', () => {
    const window = new s2r_1.CSPARQLWindow(':window1', 100, 100, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0, 10);
    const content = new s2r_1.QuadContainer(new Set(), 0);
    const target = new s2r_1.WindowInstance(0, 100);
    window.active_windows.set(target, content);
    window.set_current_time(100);
    const event = quad(namedNode('https://rsp.js/late'), namedNode('http://rsp.js/p'), namedNode('http://rsp.js/o'), defaultGraph());
    const observation = window.add(event, 90);
    expect(observation).toMatchObject({
        out_of_order: true,
        lateness_ms: 10,
        max_out_of_orderness_ms: 10,
        within_bound: true,
    });
    expect(content.elements.has(event)).toBe(true);
    expect(window.time).toBe(100);
});
test('rejects a late event beyond max_delay', () => {
    const window = new s2r_1.CSPARQLWindow(':window1', 100, 100, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0, 10);
    const content = new s2r_1.QuadContainer(new Set(), 0);
    window.active_windows.set(new s2r_1.WindowInstance(0, 100), content);
    window.set_current_time(100);
    const event = quad(namedNode('https://rsp.js/too-late'), namedNode('http://rsp.js/p'), namedNode('http://rsp.js/o'), defaultGraph());
    const observation = window.add(event, 89);
    expect(observation.within_bound).toBe(false);
    expect(content.elements.has(event)).toBe(false);
    expect(window.time).toBe(100);
});
test('uses half-open bounds for adjacent windows', () => {
    const window = new s2r_1.CSPARQLWindow(':window1', 10, 10, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0);
    const first = new s2r_1.QuadContainer(new Set(), 0);
    const second = new s2r_1.QuadContainer(new Set(), 10);
    window.active_windows.set(new s2r_1.WindowInstance(0, 10), first);
    window.active_windows.set(new s2r_1.WindowInstance(10, 20), second);
    expect(window.getContent(9)).toBe(first);
    expect(window.getContent(10)).toBe(second);
    expect(window.getContent(20)).toBeUndefined();
});
test('records centered logical trigger time while retaining actual window bounds', () => {
    const anchor = 1000;
    const window = new s2r_1.CSPARQLWindow(':window1', 120, 60, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, anchor, 0, s2r_1.WindowSemantics.Centered);
    const content = new s2r_1.QuadContainer(new Set([
        quad(namedNode('https://rsp.js/centered'), namedNode('http://rsp.js/p'), namedNode('http://rsp.js/o'), defaultGraph()),
    ]), anchor);
    window.active_windows.set(new s2r_1.WindowInstance(anchor, anchor + 120), content);
    const emitted = [];
    window.subscribe('RStream', (result) => emitted.push(result));
    window.update_watermark(anchor + 120);
    expect(emitted).toHaveLength(1);
    expect(emitted[0].window_start).toBe(anchor);
    expect(emitted[0].window_end).toBe(anchor + 120);
    expect(emitted[0].logical_trigger_time).toBe(anchor + 60);
    expect(emitted[0].window_semantics).toBe(s2r_1.WindowSemantics.Centered);
});
