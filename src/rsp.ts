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
            // @ts-ignore: Set is intentionally annotated with the graph used by the existing API.
            quadcontainer.elements._graph = require("n3").DataFactory.namedNode(window.name);
            window.add(quadcontainer.elements, quadcontainer.last_time_changed());
        });
    }

    add(event: Set<Quad>, ts: number) {
        this.emitter.emit("data", new QuadContainer(event, ts));
    }
}

export class RSPEngine {
    windows: Array<CSPARQLWindow>;
    streams: Map<string, RDFStream>;
    public max_delay: number;
    public window_semantics: WindowSemantics;
    private r2r: R2ROperator;
    private logger: Logger;

    constructor(query: string, options: RSPEngineOptions = {}) {
        this.windows = new Array<CSPARQLWindow>();
        this.streams = new Map<string, RDFStream>();
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
            window.subscribe("RStream", async (data: QuadContainer) => {
                if (data.len() === 0) {
                    return;
                }

                // A result window may depend on more than one named stream.
                // Include the content of the other active windows at the same
                // event time, using the same half-open boundary semantics.
                for (const otherWindow of this.windows) {
                    if (otherWindow === window) {
                        continue;
                    }
                    const otherContent = otherWindow.getContent(data.last_time_changed());
                    otherContent?.elements.forEach((quad) => data.add(quad, data.last_time_changed()));
                }

                this.logger.info(
                    `Processing window ${window.getCSPARQLWindowDefinition()} with ${data.len()} quads`,
                    "RSPEngine",
                );
                const bindingsStream = await this.r2r.execute(data);
                const timestampFrom = data.window_start ?? data.last_time_changed();
                const timestampTo = data.window_end ?? timestampFrom + window.width;
                const logicalTriggerTime = data.logical_trigger_time ?? timestampTo;
                const semantics = data.window_semantics ?? this.window_semantics;

                bindingsStream.on("data", (binding: any) => {
                    const result: binding_with_timestamp = {
                        bindings: binding,
                        timestamp_from: timestampFrom,
                        timestamp_to: timestampTo,
                        logical_trigger_time: logicalTriggerTime,
                        window_semantics: semantics,
                    };
                    emitter.emit("RStream", result);
                });
            });
        });
        return emitter;
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
