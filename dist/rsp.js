"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
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
exports.RSPEngine = exports.RDFStream = void 0;
const events_1 = require("events");
const LOG_CONFIG = __importStar(require("./config/log_config.json"));
const Logger_1 = require("./util/Logger");
const s2r_1 = require("./operators/s2r");
const r2r_1 = require("./operators/r2r");
const rspql_1 = require("./rspql");
class RDFStream {
    constructor(name, window) {
        this.name = name;
        this.emitter = new events_1.EventEmitter();
        this.emitter.on("data", (quadcontainer) => {
            // The graph identifies the source window for Comunica queries.
            const graph = require("n3").DataFactory.namedNode(window.name);
            quadcontainer.elements.forEach((quad) => {
                // @ts-ignore: N3 stores the graph term in its private _graph field.
                quad._graph = graph;
            });
            window.add(quadcontainer.elements, quadcontainer.last_time_changed());
        });
    }
    add(event, ts) {
        const elements = event instanceof Set ? event : new Set([event]);
        this.emitter.emit("data", new s2r_1.QuadContainer(elements, ts));
    }
}
exports.RDFStream = RDFStream;
class RSPEngine {
    constructor(query, options = {}) {
        var _a;
        this.windows = new Array();
        this.streams = new Map();
        this.max_delay = Math.max(0, (_a = options.max_delay) !== null && _a !== void 0 ? _a : 0);
        this.window_semantics = this.resolveWindowSemantics(options.window_semantics);
        const logLevel = Logger_1.LogLevel[LOG_CONFIG.log_level];
        this.logger = new Logger_1.Logger(logLevel, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination);
        const parser = new rspql_1.RSPQLParser();
        const parsedQuery = parser.parse(query);
        parsedQuery.s2r.forEach((window) => {
            const windowImplementation = new s2r_1.CSPARQLWindow(window.window_name, window.width, window.slide, s2r_1.ReportStrategy.OnWindowClose, s2r_1.Tick.TimeDriven, 0, this.max_delay, this.window_semantics);
            this.windows.push(windowImplementation);
            this.streams.set(window.stream_name, new RDFStream(window.stream_name, windowImplementation));
        });
        this.r2r = new r2r_1.R2ROperator(parsedQuery.sparql);
    }
    register() {
        const emitter = new events_1.EventEmitter();
        this.windows.forEach((window) => {
            window.subscribe("RStream", (data) => {
                void this.processWindow(window, data, emitter).catch((error) => {
                    this.reportProcessingError(emitter, error);
                });
            });
        });
        return emitter;
    }
    processWindow(window, data, emitter) {
        var _a, _b;
        return __awaiter(this, void 0, void 0, function* () {
            if (data.len() === 0) {
                return;
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
                const otherContent = exactContent !== null && exactContent !== void 0 ? exactContent : otherWindow.getContent(relatedTimestamp);
                otherContent === null || otherContent === void 0 ? void 0 : otherContent.elements.forEach((quad) => data.add(quad, relatedTimestamp));
            }
            this.logger.info(`Processing window ${window.getCSPARQLWindowDefinition()} with ${data.len()} quads`, "RSPEngine");
            const bindingsStream = yield this.r2r.execute(data);
            const timestampFrom = windowStart !== null && windowStart !== void 0 ? windowStart : relatedTimestamp;
            const timestampTo = windowEnd !== null && windowEnd !== void 0 ? windowEnd : timestampFrom + window.width;
            const logicalTriggerTime = (_a = data.logical_trigger_time) !== null && _a !== void 0 ? _a : timestampTo;
            const semantics = (_b = data.window_semantics) !== null && _b !== void 0 ? _b : this.window_semantics;
            yield new Promise((resolve, reject) => {
                let settled = false;
                const finish = (error) => {
                    if (settled) {
                        return;
                    }
                    settled = true;
                    if (error === undefined) {
                        resolve();
                    }
                    else {
                        reject(error);
                    }
                };
                bindingsStream.on("data", (binding) => {
                    try {
                        const result = {
                            bindings: binding,
                            timestamp_from: timestampFrom,
                            timestamp_to: timestampTo,
                            logical_trigger_time: logicalTriggerTime,
                            window_semantics: semantics,
                        };
                        emitter.emit("RStream", result);
                    }
                    catch (error) {
                        finish(error);
                    }
                });
                bindingsStream.on("end", () => finish());
                bindingsStream.on("error", (error) => finish(error));
            });
        });
    }
    reportProcessingError(emitter, error) {
        const normalizedError = error instanceof Error ? error : new Error(String(error));
        if (emitter.listenerCount("error") > 0) {
            emitter.emit("error", normalizedError);
            return;
        }
        this.logger.error(`RSP query processing failed: ${normalizedError.message}`, "RSPEngine");
    }
    getStream(stream_name) {
        return this.streams.get(stream_name);
    }
    addStaticData(static_data) {
        this.r2r.addStaticData(static_data);
    }
    get_all_streams() {
        const streams = [];
        this.streams.forEach((stream) => streams.push(stream.name));
        return streams;
    }
    resolveWindowSemantics(value) {
        const normalized = value;
        return (normalized === null || normalized === void 0 ? void 0 : normalized.toLowerCase()) === s2r_1.WindowSemantics.Centered
            ? s2r_1.WindowSemantics.Centered
            : s2r_1.WindowSemantics.Trailing;
    }
}
exports.RSPEngine = RSPEngine;
