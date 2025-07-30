import { QuadContainer } from "./s2r";
import { Store } from 'n3';
import { ParseOptions } from "rdf-parse/lib/RdfParser";
declare const EventEmitter: any;
import { Quad } from 'n3';
import { Logger } from "../util/Logger";
/**
 * R2R Operator Implementation Class for the RSP Engine.
 * It performs operations such as Join, Filter, Aggregation on a stream of data
 * to generate a new stream of data.
 */
export declare class R2ROperator extends EventEmitter {
    query: string;
    staticData: Set<Quad>;
    rules: string;
    logger: Logger;
    constructor(query: string, rules: string);
    /**
     * Add static data to the R2R Operator.
     * @param {Quad} quad - The quad to be added as static data.
     */
    addStaticData(quad: Quad): void;
    execute(container: QuadContainer): Promise<any>;
}
/**
 * Convert a store to a string representation.
 * @param {Store} store - The N3 Store to be converted.
 * @returns {string[]} - Array of string representations of the quads.
 */
export declare function storeToString(store: Store): string[];
export declare function stringToStore(text: string, options: ParseOptions): Promise<Store>;
export {};
