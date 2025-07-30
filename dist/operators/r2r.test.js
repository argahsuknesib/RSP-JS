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
const r2r_1 = require("./r2r");
const s2r_1 = require("./s2r");
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode, literal, defaultGraph, quad } = DataFactory;
test('test_execute_r2r', () => __awaiter(void 0, void 0, void 0, function* () {
}));
test("R2R Operator execution test", () => __awaiter(void 0, void 0, void 0, function* () {
    const operator = new r2r_1.R2ROperator("SELECT * WHERE {?s ?p ?o}", "");
    const container = new s2r_1.QuadContainer(new Set, 0);
    yield expect(operator.execute(container)).resolves.not.toThrow();
}));
test('test_query_engine', () => __awaiter(void 0, void 0, void 0, function* () {
    const rules = ``;
    let r2r = new r2r_1.R2ROperator(`SELECT ?p WHERE { ?s ?p ?o }`, rules);
    const quad1 = quad(namedNode('https://rsp.js/test_subject_0'), namedNode('http://rsp.js/test_property'), namedNode('http://rsp.js/test_object'), defaultGraph());
    const quad2 = quad(namedNode('https://rsp.js/test_subject_1'), namedNode('http://rsp.js/test_property'), namedNode('http://rsp.js/test_object'), defaultGraph());
    let quadSet = new Set();
    quadSet.add(quad1);
    quadSet.add(quad2);
    let container = new s2r_1.QuadContainer(quadSet, 0);
    const bindingsStream = yield r2r.execute(container);
    let resuults = new Array();
    // @ts-ignore
    bindingsStream.on('data', (binding) => {
        console.log(binding.toString()); // Quick way to print bindings for testing
        resuults.push(binding.toString());
    });
    bindingsStream.on('end', () => {
        // The data-listener will not be called anymore once we get here.
        expect(resuults.length).toBe(2);
    });
}));
