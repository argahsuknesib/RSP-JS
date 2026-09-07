/// <reference types="node" />
import { EventEmitter } from "events";
import { Quad } from "n3";
import { CSPARQLWindow, WindowSemantics } from "./operators/s2r";
export type RSPEngineOptions = {
    max_delay?: number;
    window_semantics?: WindowSemantics | "trailing" | "centered";
};
export type binding_with_timestamp = {
    bindings: any;
    timestamp_from: number;
    timestamp_to: number;
    logical_trigger_time?: number;
    window_semantics?: WindowSemantics;
};
export declare class RDFStream {
    name: string;
    emitter: EventEmitter;
    constructor(name: string, window: CSPARQLWindow);
    add(event: Quad | Set<Quad>, ts: number): void;
}
export declare class RSPEngine {
    windows: Array<CSPARQLWindow>;
    streams: Map<string, RDFStream>;
    max_delay: number;
    window_semantics: WindowSemantics;
    private r2r;
    private logger;
    private processing_queue;
    constructor(query: string, options?: RSPEngineOptions);
    register(): EventEmitter<[never]>;
    private processWindow;
    private reportProcessingError;
    getStream(stream_name: string): RDFStream | undefined;
    addStaticData(static_data: Quad): void;
    get_all_streams(): string[];
    private resolveWindowSemantics;
}
