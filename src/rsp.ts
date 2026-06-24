import { CSPARQLWindow, QuadContainer, ReportStrategy, Tick, WindowSemantics } from "./operators/s2r";
import { R2ROperator } from "./operators/r2r";
import { EventEmitter } from "events";
import * as LOG_CONFIG from "./config/log_config.json";
import { Logger } from "./util/Logger";
import { LogLevel, LogDestination } from "./util/LoggerEnum";
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode } = DataFactory;
import { LogLevel as LogLevelEnum } from "./util/LoggerEnum";
import { LogDestination as LogDestinationEnum } from "./util/LoggerEnum";
// @ts-ignore
import { Quad } from 'n3';
import { RSPQLParser, WindowDefinition } from "./rspql";

export type binding_with_timestamp = {
    bindings: any,
    timestamp_from: number,
    timestamp_to: number,
    logical_trigger_time: number,
    window_start: number,
    window_end: number,
    window_data_close_time: number,
    result_emitted_at: number,
    latency_from_logical_trigger_ms: number,
    latency_from_window_close_ms: number,
    window_number: number,
    window_name: string,
    window_semantics: WindowSemantics
}

export type RSPEngineOptions = {
    max_delay?: number,
    window_semantics?: WindowSemantics | "trailing" | "centered"
}

/**
 * RDF Stream Class to represent the stream of RDF Data.   
 * It emits the data to the CSPARQL Window for processing.
 */
export class RDFStream {
    name: string;
    emitter: EventEmitter;

    /**
     * Constructor for the RDFStream class.
     * @param {string} name - The name of the stream to be created.
     * @param {CSPARQLWindow} window - The CSPARQL Window to which the stream is to be processed and emitted by the S2R Operator.
     */
    constructor(name: string, window: CSPARQLWindow) {
        this.name = name;
        const EventEmitter = require('events').EventEmitter;
        this.emitter = new EventEmitter();
        this.emitter.on('data', (quadcontainer: QuadContainer) => {
            // @ts-ignore
            quadcontainer.elements._graph = namedNode(window.name);
            // @ts-ignore
            window.add(quadcontainer.elements, quadcontainer.last_time_changed());
        });
    }

    /**
     * Adds the event to the RDF Stream to be processed by the RSP Engine.
     * @param {Set<Quad>} event - The event to be added to the stream. The event is a set of quads of the form {subject, predicate, object, graph}.
     * @param {number} ts - The timestamp of the event.
     */
    add(event: Set<Quad>, ts: number) {
        this.emitter.emit('data', new QuadContainer(event, ts));
    }
}

/**
 * RSPEngine Class to represent the RSP Engine.
 * It contains the windows and streams of the RSP Engine.
 */
export class RSPEngine {
    windows: Array<CSPARQLWindow>;
    streams: Map<string, RDFStream>;
    public max_delay: number;
    public window_semantics: WindowSemantics;
    public log_enabled!: boolean;
    private r2r: R2ROperator;
    public logger: Logger;

    /**
     * Constructor for the RSPEngine class.
     * @param {string} query - The query to be executed by the RSP Engine.
     * @param {{max_delay: number }} opts - The options for the RSP Engine for processing the data if they are late or out of order.
     * @param {number} opts.max_delay - The maximum delay for the window to be processed in the case of late data arrival and out of order data.
     * This field is optional and defaults to 0 for no delay expected by the RSP Engine in processing of the data.
     */
    constructor(query: string, opts?: RSPEngineOptions) {
        this.windows = new Array<CSPARQLWindow>();
        if (opts) {
            this.max_delay = opts.max_delay ? opts.max_delay : 0;
        }
        else {
            this.max_delay = 0;
        }
        this.window_semantics = this.resolveWindowSemantics((opts?.window_semantics ?? process.env.RSP_WINDOW_SEMANTICS) as string | undefined);
        const logLevel: LogLevel = LogLevel[LOG_CONFIG.log_level as keyof typeof LogLevel];
        this.logger = new Logger(logLevel, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination as unknown as LogDestination);      
        this.streams = new Map<string, RDFStream>();
        const parser = new RSPQLParser();
        const parsed_query = parser.parse(query);
        parsed_query.s2r.forEach((window: WindowDefinition) => {
            const windowImpl = new CSPARQLWindow(window.window_name, window.width, window.slide, ReportStrategy.OnWindowClose, Tick.TimeDriven, 0, this.max_delay, this.window_semantics);
            this.windows.push(windowImpl);
            const stream = new RDFStream(window.stream_name, windowImpl);
            this.streams.set(window.stream_name, stream);
        })
        this.r2r = new R2ROperator(parsed_query.sparql);

    }
    /**
     * Register the RSP Engine to start processing the data.
     * @returns {any} - The event emitter to emit the data to the RSP Engine.
     */
    register() {
        const EventEmitter = require('events').EventEmitter;
        const emitter = new EventEmitter();
        this.windows.forEach((window) => {            
            window.subscribe("RStream", async (data: QuadContainer) => {
                if (data) {
                    if (data.len() > 0) {
                        // this.logger.info(`Received window content for time ${data.last_time_changed()}`, `RSPEngine`);
                        // iterate over all the windows
                        for (const windowIt of this.windows) {
                            // filter out the current triggering one
                            if (windowIt != window) {
                                const currentWindowData = windowIt.getContent(data.last_time_changed());                                
                            //    this.logger.info(`Window Content ${data.len()} for time ${data.last_time_changed()} for window ${windowIt.getCSPARQLWindowDefinition()}`, `RSPEngine`);
                                if (currentWindowData) {
                                    // add the content of the other windows to the quad container
                              //      this.logger.info(`Data length before adding ${data.len()}`, `RSPEngine`);            
                                    currentWindowData.elements.forEach((q) => data.add(q, data.last_time_changed()));
                               //     this.logger.info(`Data length after adding ${data.len()}`, `RSPEngine`);            
                                }
                            }
                        }
                        this.logger.info(`Starting Window Query Processing for the window ${window.getCSPARQLWindowDefinition()} with window size ${data.len()}`, `RSPEngine`);
                        let time_start_query_processing = new Date().getTime();
                        const bindingsStream = await this.r2r.execute(data);
                        let time_end_query_processing = new Date().getTime();
                        this.logger.info(`Ended the execution of the R2R Operator for the window ${window.getCSPARQLWindowDefinition()} with window size ${data.len()}`, `RSPEngine`);
                        // this.logger.info(`Time taken for query processing for window ${window.getCSPARQLWindowDefinition()} is ${time_end_query_processing - time_start_query_processing} ms with window size ${data.len()}`, `RSPEngine`);
                        const timestamp_from = data.window_start ?? window.t0;
                        const timestamp_to = data.window_end ?? (window.t0 + window.slide);
                        const logical_trigger_time = data.logical_trigger_time ?? timestamp_to;
                        const window_data_close_time = data.window_data_close_time ?? timestamp_to;
                        const result_emitted_at = data.result_emitted_at ?? window_data_close_time;
                        const latency_from_logical_trigger_ms = data.latency_from_logical_trigger_ms ?? (result_emitted_at - logical_trigger_time);
                        const latency_from_window_close_ms = data.latency_from_window_close_ms ?? (result_emitted_at - window_data_close_time);
                        const window_number = data.window_number ?? 0;
                        const window_name = data.window_name ?? window.name;
                        const window_semantics = data.window_semantics ?? this.window_semantics;
                        const emissionKey = [
                            window_name,
                            window_number,
                            timestamp_from,
                            timestamp_to,
                            result_emitted_at,
                            window_semantics
                        ].join("|");
                        if ((window as any)._seen_emission_keys === undefined) {
                            (window as any)._seen_emission_keys = new Set<string>();
                        }
                        const seenEmissionKeys = (window as any)._seen_emission_keys as Set<string>;
                        if (seenEmissionKeys.has(emissionKey)) {
                            return;
                        }
                        seenEmissionKeys.add(emissionKey);
                        bindingsStream.on('data', (binding: any) => {
                            const object_with_timestamp: binding_with_timestamp = {
                                bindings: binding,
                                timestamp_from,
                                timestamp_to,
                                logical_trigger_time,
                                window_start: timestamp_from,
                                window_end: timestamp_to,
                                window_data_close_time,
                                result_emitted_at,
                                latency_from_logical_trigger_ms,
                                latency_from_window_close_ms,
                                window_number,
                                window_name,
                                window_semantics
                            }
                            window.t0 += window.slide;
                            emitter.emit("RStream", object_with_timestamp);
                        });
                        bindingsStream.on('end', () => {
                         //   this.logger.info(`Ended Comunica Binding Stream for window ${window.getCSPARQLWindowDefinition()} with window size ${data.len()}`, `RSPEngine`);
                        });
                    }
                }
            });
        });
        return emitter;
    }

    /**
     * Get the stream by the stream name.
     * @param {string} stream_name - The name of the stream to be fetched.
     * @returns {RDFStream | undefined} - The stream with the given name.
     */
    getStream(stream_name: string) {
        return this.streams.get(stream_name);
    }

    /**
     * Add static data to the RSP Engine.
     * @param {Quad} static_data - The static data to be added to the RSP Engine.
     */
    addStaticData(static_data: Quad) {
        this.r2r.addStaticData(static_data);
    }

    /**
     * Get all the streams of the RSP Engine.
     * @returns {string[]} - The list of all the streams in the RSP Engine.
     */
    get_all_streams() {
        const streams: string[] = [];
        this.streams.forEach((stream) => {
            streams.push(stream.name);
        });
        return streams;
    }

    private resolveWindowSemantics(value?: string): WindowSemantics {
        if (value && value.toLowerCase() === WindowSemantics.Centered) {
            return WindowSemantics.Centered;
        }
        return WindowSemantics.Trailing;
    }


}
