import { CSPARQLWindow, QuadContainer, ReportStrategy, Tick } from "./operators/s2r";
import { R2ROperator } from "./operators/r2r";
import { EventEmitter } from "events";
import * as LOG_CONFIG from "./config/log_config.json";
import * as LOG_ENGINE from "./config/log_engine.json";
import { Logger } from "./util/Logger";
import { LogLevel, LogDestination } from "./util/LoggerEnum";
const N3 = require('n3');
const { DataFactory } = N3;
const { namedNode } = DataFactory;
// @ts-ignore
import { Quad } from 'n3';
import { RSPQLParser, WindowDefinition } from "./rspql";

export type binding_with_timestamp = {
    bindings: any;
    timestamp_from: number;
    timestamp_to: number;
    definition: string;
};

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
    private r2r: R2ROperator;
    public logger: Logger;

    constructor(query: string, opts?: { max_delay?: number }) {
        this.windows = [];
        this.max_delay = opts?.max_delay ?? 0;
        const logLevel: LogLevel = LogLevel[LOG_CONFIG.log_level as keyof typeof LogLevel];
        this.logger = new Logger(logLevel, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination as unknown as LogDestination);
        this.streams = new Map<string, RDFStream>();

        const parser = new RSPQLParser();
        const parsed_query = parser.parse(query);

        parsed_query.s2r.forEach((window: WindowDefinition) => {
            const windowImpl = new CSPARQLWindow(
                window.window_name,
                window.width,
                window.slide,
                ReportStrategy.OnWindowClose,
                Tick.TimeDriven,
                0,
                this.max_delay,
                LOG_ENGINE.trigger_threshold
            );
            this.windows.push(windowImpl);
            const stream = new RDFStream(window.stream_name, windowImpl);
            this.streams.set(window.stream_name, stream);
        });

        this.r2r = new R2ROperator(parsed_query.sparql);
    }

    /**
     * Register the RSP Engine to start processing the data.
     * @returns {EventEmitter} - The event emitter to emit the processed data.
     */
    register(): EventEmitter {
        const emitter = new EventEmitter();
        this.windows.forEach((window) => {
            window.subscribe("RStream", async (data: QuadContainer) => {
                if (data && data.len() > 0) {
                    await this.processTrigger(window, this.windows, data, LOG_ENGINE.trigger_threshold || 1);
                    this.logger.info(
                        `Starting Query Processing for ${window.getCSPARQLWindowDefinition()} with size ${data.len()}`,
                        "RSPEngine"
                    );

                    const bindingsStream = await this.r2r.execute(data);
                    bindingsStream.on("data", (binding: any) => {
                        const object_with_timestamp: binding_with_timestamp = {
                            bindings: binding,
                            timestamp_from: window.t0,
                            timestamp_to: window.t0 + window.slide,
                            definition: window.getCSPARQLWindowDefinition()
                        };
                        window.t0 += window.slide;
                        emitter.emit("RStream", object_with_timestamp);
                    });
                }
            });
        });
        return emitter;
    }

    /**
     * Wait for data in a window to meet the target length using an event-driven approach.
     * @param {CSPARQLWindow} windowIt - The window to monitor for data.
     * @param {number} targetLength - The target number of elements.
     * @param {number} timestamp - The timestamp for the data.
     * @returns {Promise<QuadContainer>} - A promise that resolves when the condition is met.
     */
    async waitForWindowDataWithEmitter(
        windowIt: CSPARQLWindow,
        targetLength: number,
        timestamp: number
    ): Promise<QuadContainer> {
        return new Promise((resolve) => {
            const checkCondition = () => {
                const currentWindowData = windowIt.getContent(timestamp);
                if (currentWindowData && currentWindowData.elements.size === targetLength) {
                    resolve(currentWindowData);
                    windowIt.off("data", checkCondition); // Prevent memory leaks
                }
            };

            windowIt.on("data", checkCondition);
        });
    }

    /**
     * Process a triggering window and integrate data from other windows.
     * @param {CSPARQLWindow} window - The triggering window.
     * @param {CSPARQLWindow[]} allWindows - All windows in the RSP Engine.
     * @param {QuadContainer} data - The data from the triggering window.
     * @param {number} targetLength - The target length for the data.
     */
    async processTrigger(
        window: CSPARQLWindow,
        allWindows: CSPARQLWindow[],
        data: QuadContainer,
        targetLength: number
    ) {
        for (const windowIt of allWindows) {
            if (windowIt !== window) {
                const timestamp = data.last_time_changed();
                const currentWindowData = await this.waitForWindowDataWithEmitter(windowIt, targetLength, timestamp);
                currentWindowData.elements.forEach((q: Quad) => data.add(q, timestamp));
            }
        }
    }

    getStream(stream_name: string): RDFStream | undefined {
        return this.streams.get(stream_name);
    }

    addStaticData(static_data: Quad): void {
        this.r2r.addStaticData(static_data);
    }

    get_all_streams(): string[] {
        return Array.from(this.streams.keys());
    }
}
