import { R2ROperator } from './r2r';
import { QuadContainer } from "./s2r";
import { Quad } from 'n3';
const N3 = require('n3');

const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;
test('test_execute_r2r', async () => {
});

test("R2R Operator execution test", async () => {
    const operator = new R2ROperator("SELECT * WHERE {?s ?p ?o}", "");
    const container = new QuadContainer(new Set<Quad>, 0);
    await expect(operator.execute(container)).resolves.not.toThrow();
});


test('test_query_engine', async () => {
    const rules = ``;
    let r2r = new R2ROperator(`SELECT ?p WHERE { ?s ?p ?o }`, rules);
    const quad1 = quad(
        namedNode('https://rsp.js/test_subject_0'),
        namedNode('http://rsp.js/test_property'),
        namedNode('http://rsp.js/test_object'),
        defaultGraph(),
    );
    const quad2 = quad(
        namedNode('https://rsp.js/test_subject_1'),
        namedNode('http://rsp.js/test_property'),
        namedNode('http://rsp.js/test_object'),
        defaultGraph(),
    );
    let quadSet = new Set<Quad>();
    quadSet.add(quad1);
    quadSet.add(quad2);
    let container = new QuadContainer(quadSet, 0);
    const bindingsStream = await r2r.execute(container);
    let resuults = new Array<string>();
    // @ts-ignore
    bindingsStream.on('data', (binding) => {
        console.log(binding.toString()); // Quick way to print bindings for testing

        resuults.push(binding.toString());
    });
    bindingsStream.on('end', () => {
        // The data-listener will not be called anymore once we get here.
        expect(resuults.length).toBe(2);

    });
});
