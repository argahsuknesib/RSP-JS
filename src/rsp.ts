import { EventEmitter } from "events";
import * as LOG_CONFIG from "./config/log_config.json";
import { LogDestination, LogLevel, Logger } from "./util/Logger";
import { Quad } from "n3";
import { CSPARQLWindow, QuadContainer, ReportStrategy, Tick, WindowSemantics } from "./operators/s2r";
import { R2ROperator } from "./operators/r2r";
import { RSPQLParser, WindowDefinition } from "./rspql";

export type RSPEngineOptions = {
    max_delay?: number,
    window_semantics?: WindowSemantics | "trailing" | "centered",
};

export type binding_with_timestamp = {
    bindings: any,
    timestamp_from: number,
    timestamp_to: number,
    logical_trigger_time?: number,
    window_semantics?: WindowSemantics,
};

export class RDFStream {
    name: string;
    emitter: EventEmitter;

    constructor(name: string, window: CSPARQLWindow) {
        this.name = name;
        this.emitter = new EventEmitter();
        this.emitter.on("data", (quadcontainer: QuadContainer) => {
            // The graph identifies the source window for Comunica queries.
            const graph = require("n3").DataFactory.namedNode(window.name);
            quadcontainer.elements.forEach((quad) => {
                // @ts-ignore: N3 stores the graph term in its private _graph field.
                quad._graph = graph;
            });
            window.add(quadcontainer.elements, quadcontainer.last_time_changed());
        });
    }

    add(event: Quad | Set<Quad>, ts: number) {
        const elements = event instanceof Set ? event : new Set<Quad>([event]);
        this.emitter.emit("data", new QuadContainer(elements, ts));
    }
}

export class RSPEngine {
    windows: Array<CSPARQLWindow>;
    streams: Map<string, RDFStream>;
    public max_delay: number;
    public window_semantics: WindowSemantics;
    private r2r: R2ROperator;
    private logger: Logger;
    private next_processing_sequence: number;
    private next_emission_sequence: number;
    private completed_processing: Map<number, { results: binding_with_timestamp[], error?: unknown }>;

    constructor(query: string, options: RSPEngineOptions = {}) {
        this.windows = new Array<CSPARQLWindow>();
        this.streams = new Map<string, RDFStream>();
        this.next_processing_sequence = 0;
        this.next_emission_sequence = 0;
        this.completed_processing = new Map();
        this.max_delay = Math.max(0, options.max_delay ?? 0);
        this.window_semantics = this.resolveWindowSemantics(options.window_semantics);

        const logLevel = LogLevel[LOG_CONFIG.log_level as keyof typeof LogLevel];
        this.logger = new Logger(
            logLevel,
            LOG_CONFIG.classes_to_log,
            LOG_CONFIG.destination as unknown as LogDestination,
        );

        const parser = new RSPQLParser();
        const parsedQuery = parser.parse(query);
        parsedQuery.s2r.forEach((window: WindowDefinition) => {
            const windowImplementation = new CSPARQLWindow(
                window.window_name,
                window.width,
                window.slide,
                ReportStrategy.OnWindowClose,
                Tick.TimeDriven,
                0,
                this.max_delay,
                this.window_semantics,
            );
            this.windows.push(windowImplementation);
            this.streams.set(window.stream_name, new RDFStream(window.stream_name, windowImplementation));
        });
        this.r2r = new R2ROperator(parsedQuery.sparql);
    }

    register() {
        const emitter = new EventEmitter();
        this.windows.forEach((window) => {
            window.subscribe("RStream", (data: QuadContainer) => {
                const sequence = this.next_processing_sequence++;
                void this.processWindow(window, data)
                    .then((results) => this.completeProcessing(emitter, sequence, results))
                    .catch((error: unknown) => this.completeProcessing(emitter, sequence, [], error));
            });
        });
        return emitter;
    }

    private async processWindow(window: CSPARQLWindow, data: QuadContainer): Promise<binding_with_timestamp[]> {
        if (data.len() === 0) {
            return [];
        }

        // A result window may depend on more than one named stream. Prefer the
        // exact logical window bounds over the last event that mutated content;
        // the latter can be an out-of-order timestamp.
        const windowStart = data.window_start;
        const windowEnd = data.window_end;
        const relatedTimestamp = data.last_time_changed();
        for (const otherWindow of this.windows) {
            if (otherWindow === window) {
                continue;
            }
            const exactContent = windowStart !== undefined && windowEnd !== undefined
                ? otherWindow.getContentForWindow(windowStart, windowEnd)
                : undefined;
            const otherContent = exactContent ?? otherWindow.getContent(relatedTimestamp);
            otherContent?.elements.forEach((quad) => data.add(quad, relatedTimestamp));
        }

        this.logger.info(
            `Processing window ${window.getCSPARQLWindowDefinition()} with ${data.len()} quads`,
            "RSPEngine",
        );
        const bindingsStream = await this.r2r.execute(data);
        const timestampFrom = windowStart ?? relatedTimestamp;
        const timestampTo = windowEnd ?? timestampFrom + window.width;
        const logicalTriggerTime = data.logical_trigger_time ?? timestampTo;
        const semantics = data.window_semantics ?? this.window_semantics;
        const results: binding_with_timestamp[] = [];

        await new Promise<void>((resolve, reject) => {
            let settled = false;
            const finish = (error?: unknown) => {
                if (settled) {
                    return;
                }
                settled = true;
                if (error === undefined) {
                    resolve();
                } else {
                    reject(error);
                }
            };

            bindingsStream.on("data", (binding: any) => {
                results.push({
                    bindings: binding,
                    timestamp_from: timestampFrom,
                    timestamp_to: timestampTo,
                    logical_trigger_time: logicalTriggerTime,
                    window_semantics: semantics,
                });
            });
            bindingsStream.on("end", () => finish());
            bindingsStream.on("error", (error: unknown) => finish(error));
        });
        return results;
    }

    private completeProcessing(
        emitter: EventEmitter,
        sequence: number,
        results: binding_with_timestamp[],
        error?: unknown,
    ): void {
        this.completed_processing.set(sequence, { results, error });

        while (this.completed_processing.has(this.next_emission_sequence)) {
            const completed = this.completed_processing.get(this.next_emission_sequence);
            this.completed_processing.delete(this.next_emission_sequence);
            this.next_emission_sequence++;
            if (completed === undefined) {
                continue;
            }

            if (completed.error !== undefined) {
                this.reportProcessingErrorSafely(emitter, completed.error);
                continue;
            }

            for (const result of completed.results) {
                try {
                    emitter.emit("RStream", result);
                } catch (emissionError) {
                    this.reportProcessingErrorSafely(emitter, emissionError);
                }
            }
        }
    }

    private reportProcessingErrorSafely(emitter: EventEmitter, error: unknown): void {
        try {
            this.reportProcessingError(emitter, error);
        } catch (reportingError) {
            this.logger.error(
                `RSP query processing failed: ${String(reportingError)}`,
                "RSPEngine",
            );
        }
    }

    private reportProcessingError(emitter: EventEmitter, error: unknown): void {
        const normalizedError = error instanceof Error ? error : new Error(String(error));
        if (emitter.listenerCount("error") > 0) {
            emitter.emit("error", normalizedError);
            return;
        }
        this.logger.error(`RSP query processing failed: ${normalizedError.message}`, "RSPEngine");
    }

    getStream(stream_name: string) {
        return this.streams.get(stream_name);
    }

    addStaticData(static_data: Quad) {
        this.r2r.addStaticData(static_data);
    }

    get_all_streams() {
        const streams: string[] = [];
        this.streams.forEach((stream) => streams.push(stream.name));
        return streams;
    }

    private resolveWindowSemantics(value?: WindowSemantics | "trailing" | "centered") {
        const normalized = value as string | undefined;
        return normalized?.toLowerCase() === WindowSemantics.Centered
            ? WindowSemantics.Centered
            : WindowSemantics.Trailing;
    }
}
