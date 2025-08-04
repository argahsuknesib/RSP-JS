import { QuadContainer } from "./s2r";
import { DataFactory } from "rdf-data-factory";
import { Logger, LogDestination, LogLevel } from "../util/Logger";
import * as LOG_CONFIG from "../config/log_config.json";
const rdfParser = require("rdf-parse").default;
const storeStream = require("rdf-store-stream").storeStream;
const streamifyString = require('streamify-string');
import { Store, Writer } from 'n3';
const N3 = require('n3');
const DF = new DataFactory();

// @ts-ignore
import { Literal, Quad } from 'n3';
export class R2ROperator {
    query: string;
    logger: Logger;
    staticData: Set<Quad>;
    constructor(query: string) {
        this.query = query;
        this.staticData = new Set<Quad>();
        const log_level: LogLevel = LogLevel[LOG_CONFIG.log_level as keyof typeof LogLevel];
        this.logger = new Logger(log_level, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination as unknown as LogDestination);
        this.logger.info("R2ROperator initialized with query: " + query, "R2ROperator");
    }
    addStaticData(quad: Quad) {
        this.staticData.add(quad);
    }
    async execute(container: QuadContainer) {
        const store = new N3.Store();
        for (let elem of container.elements) {
            store.addQuad(elem);

        }
        for (let elem of this.staticData) {
            store.addQuad(elem);
        }

        this.logger.info("Executing R2R query on store with " + store.size + " quads", "R2ROperator");
        this.logger.info("Query: " + this.query, "R2ROperator");
        this.logger.info(`${storeToString(store)}`, "R2ROperator");

        const QueryEngine = require('@comunica/query-sparql').QueryEngine;

        const myEngine = new QueryEngine();
        return await myEngine.queryBindings(this.query, {
            sources: [store],
            extensionFunctions: {
                'http://extension.org/functions#sqrt'(args: any) {
                    const arg = args[0];
                    if (arg.termType === 'Literal') {
                        return DF.literal(Math.sqrt(Number(arg.value)).toString());
                    }
                },
                'http://extension.org/functions#pow'(args: any) {
                    const arg1 = args[0];
                    if (arg1.termType === 'Literal') {
                        const arg2 = args[1];
                        if (arg2.termType === 'Literal') {
                            return DF.literal(Math.pow(Number(arg1.value), Number(arg2.value)).toString());
                        }
                    }
                }
            },
        });
    }

}

/**
* Convert a store to a string representation.
* @param {Store} store - The N3 Store to be converted.
* @returns {string[]} - Array of string representations of the quads.
*/
export function storeToString(store: Store): string[] {
    const writer = new Writer();
    return store.getQuads(null, null, null, null).map(quad => writer.quadToString(quad.subject, quad.predicate, quad.object, quad.graph));
}