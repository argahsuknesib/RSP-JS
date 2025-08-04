import { QuadContainer } from "./s2r";
import { Logger } from "../util/Logger";
import { Store } from 'n3';
import { Quad } from 'n3';
export declare class R2ROperator {
    query: string;
    logger: Logger;
    staticData: Set<Quad>;
    constructor(query: string);
    addStaticData(quad: Quad): void;
    execute(container: QuadContainer): Promise<any>;
}
/**
* Convert a store to a string representation.
* @param {Store} store - The N3 Store to be converted.
* @returns {string[]} - Array of string representations of the quads.
*/
export declare function storeToString(store: Store): string[];
