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
Object.defineProperty(exports, "__esModule", { value: true });
exports.computeWindowIfAbsent = exports.CSPARQLWindow = exports.QuadContainer = exports.WindowInstance = exports.WindowSemantics = exports.Tick = exports.ReportStrategy = void 0;
const events_1 = require("events");
const LOG_CONFIG = __importStar(require("../config/log_config.json"));
const Logger_1 = require("../util/Logger");
var ReportStrategy;
(function (ReportStrategy) {
    ReportStrategy[ReportStrategy["NonEmptyContent"] = 0] = "NonEmptyContent";
    ReportStrategy[ReportStrategy["OnContentChange"] = 1] = "OnContentChange";
    ReportStrategy[ReportStrategy["OnWindowClose"] = 2] = "OnWindowClose";
    ReportStrategy[ReportStrategy["Periodic"] = 3] = "Periodic";
})(ReportStrategy = exports.ReportStrategy || (exports.ReportStrategy = {}));
var Tick;
(function (Tick) {
    Tick[Tick["TimeDriven"] = 0] = "TimeDriven";
    Tick[Tick["TupleDriven"] = 1] = "TupleDriven";
    Tick[Tick["BatchDriven"] = 2] = "BatchDriven";
})(Tick = exports.Tick || (exports.Tick = {}));
/** The timestamp used to describe a completed window in an output result. */
var WindowSemantics;
(function (WindowSemantics) {
    WindowSemantics["Trailing"] = "trailing";
    WindowSemantics["Centered"] = "centered";
})(WindowSemantics = exports.WindowSemantics || (exports.WindowSemantics = {}));
class WindowInstance {
    constructor(open, close) {
        this.open = open;
        this.close = close;
    }
    getDefinition() {
        return `[${this.open},${this.close})`;
    }
    hasCode() {
        return 0;
    }
    is_same(other_window) {
        return this.open === other_window.open && this.close === other_window.close;
    }
}
exports.WindowInstance = WindowInstance;
class QuadContainer {
    constructor(elements, ts, window_start, window_end) {
        this.elements = elements;
        this.last_time_stamp_changed = ts;
        this.window_start = window_start;
        this.window_end = window_end;
    }
    len() {
        return this.elements.size;
    }
    add(quad, quad_timestamp) {
        if (this.elements.has(quad)) {
            return false;
        }
        this.elements.add(quad);
        this.last_time_stamp_changed = quad_timestamp;
        return true;
    }
    last_time_changed() {
        return this.last_time_stamp_changed;
    }
}
exports.QuadContainer = QuadContainer;
class CSPARQLWindow {
    constructor(name, width, slide, report, tick, start_time, max_delay = 0, window_semantics = WindowSemantics.Trailing) {
        this.name = name;
        this.width = width;
        this.slide = slide;
        this.report = report;
        this.tick = tick;
        this.time = start_time;
        this.current_watermark = start_time;
        this.t0 = start_time;
        this.max_delay = Math.max(0, max_delay);
        this.window_semantics = window_semantics;
        this.active_windows = new Map();
        this.emitter = new events_1.EventEmitter();
        this.pending_triggers = new Set();
        const logLevel = Logger_1.LogLevel[LOG_CONFIG.log_level];
        this.logger = new Logger_1.Logger(logLevel, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination);
    }
    /** Return the active window with the earliest close that contains a timestamp. */
    getContent(timestamp) {
        let selected;
        let earliestClose = Number.MAX_SAFE_INTEGER;
        for (const window of this.active_windows.keys()) {
            if (window.open <= timestamp && timestamp < window.close && window.close < earliestClose) {
                selected = window;
                earliestClose = window.close;
            }
        }
        return selected === undefined ? undefined : this.active_windows.get(selected);
    }
    /** Return the active content for one exact logical window interval. */
    getContentForWindow(window_start, window_end) {
        for (const [window, content] of this.active_windows.entries()) {
            if (window.open === window_start && window.close === window_end) {
                return content;
            }
        }
        return undefined;
    }
    /** Add one event or a set of events to all matching windows. */
    add(event, timestamp) {
        const referenceTime = this.time;
        const outOfOrder = timestamp < referenceTime;
        const lateness = outOfOrder ? referenceTime - timestamp : 0;
        const withinBound = !outOfOrder || lateness <= this.max_delay;
        const observation = {
            event_time_ms: timestamp,
            reference_time_ms: referenceTime,
            out_of_order: outOfOrder,
            lateness_ms: lateness,
            max_out_of_orderness_ms: this.max_delay,
            within_bound: withinBound,
        };
        if (!withinBound) {
            this.logger.info(`Discarding out-of-order event at ${timestamp} (${lateness}ms late)`, "CSPARQLWindow");
            return observation;
        }
        if (outOfOrder) {
            this.logger.info(`Accepting out-of-order event at ${timestamp} (${lateness}ms late)`, "CSPARQLWindow");
        }
        else {
            this.time = timestamp;
            this.logger.debug(`Accepting in-order event at ${timestamp}`, "CSPARQLWindow");
            this.scope(timestamp);
        }
        const quads = event instanceof Set ? event : new Set([event]);
        for (const window of this.active_windows.keys()) {
            if (timestamp < window.open || timestamp >= window.close) {
                continue;
            }
            const content = this.active_windows.get(window);
            if (content === undefined) {
                continue;
            }
            for (const quad of quads) {
                if (content.add(quad, timestamp)) {
                    this.pending_triggers.add(window);
                }
            }
        }
        // Late events never move the watermark backwards. In-order events
        // advance it by the configured allowed lateness.
        if (!outOfOrder) {
            this.update_watermark(timestamp - this.max_delay);
        }
        return observation;
    }
    if_event_late(timestamp) {
        return timestamp < this.time;
    }
    compute_report(window, _content, watermark) {
        if (this.report === ReportStrategy.OnWindowClose) {
            return window.close <= watermark;
        }
        if (this.report === ReportStrategy.OnContentChange) {
            return this.pending_triggers.has(window) && _content.len() > 0;
        }
        return false;
    }
    /** Emit changed content and retire windows whose close is covered by the watermark. */
    trigger_window_content(watermark) {
        const windowsToRetire = [];
        for (const [window, content] of this.active_windows.entries()) {
            const windowClosed = window.close <= watermark;
            if (this.report === ReportStrategy.OnContentChange) {
                if (this.tick === Tick.TimeDriven && this.compute_report(window, content, watermark)) {
                    this.annotateAndEmit(window, content);
                    this.pending_triggers.delete(window);
                }
                if (windowClosed) {
                    windowsToRetire.push(window);
                }
                continue;
            }
            if (!windowClosed) {
                continue;
            }
            if (this.tick === Tick.TimeDriven && this.compute_report(window, content, watermark) && content.len() > 0) {
                this.annotateAndEmit(window, content);
            }
            windowsToRetire.push(window);
        }
        for (const window of windowsToRetire) {
            this.active_windows.delete(window);
            this.pending_triggers.delete(window);
        }
    }
    annotateAndEmit(window, content) {
        content.window_start = window.open;
        content.window_end = window.close;
        content.logical_trigger_time = this.getLogicalTriggerTime(window);
        content.window_semantics = this.window_semantics;
        this.emitter.emit("RStream", content);
    }
    update_watermark(new_time) {
        if (new_time <= this.current_watermark) {
            return;
        }
        this.current_watermark = new_time;
        this.trigger_window_content(this.current_watermark);
    }
    get_current_watermark() {
        return this.current_watermark;
    }
    scope(timestamp) {
        const firstAlignedStart = Math.floor((timestamp - this.t0) / this.slide) * this.slide + this.t0;
        let windowStart = firstAlignedStart - this.width;
        while (windowStart <= timestamp) {
            computeWindowIfAbsent(this.active_windows, new WindowInstance(windowStart, windowStart + this.width), (window) => new QuadContainer(new Set(), 0, window.open, window.close));
            windowStart += this.slide;
        }
    }
    getLogicalTriggerTime(window) {
        if (this.window_semantics === WindowSemantics.Centered) {
            return window.open + Math.floor(this.width / 2);
        }
        return window.close;
    }
    subscribe(output, call_back) {
        this.emitter.on(output, call_back);
    }
    set_current_time(time) {
        this.time = Math.max(this.time, time);
    }
    set_max_delay(delay) {
        this.max_delay = Math.max(0, delay);
    }
    set_current_watermark(time) {
        this.update_watermark(time);
    }
    getCSPARQLWindowDefinition() {
        const windowDefinitions = [];
        for (const window of this.active_windows.keys()) {
            windowDefinitions.push(window.getDefinition());
        }
        return `CSPARQLWindow { name: ${this.name}, width: ${this.width}, slide: ${this.slide}, current_time: ${this.time}, current_watermark: ${this.current_watermark}, active_windows: [${windowDefinitions.join(", ")}] }`;
    }
}
exports.CSPARQLWindow = CSPARQLWindow;
function computeWindowIfAbsent(map, window, mappingFunction) {
    for (const existing of map.keys()) {
        if (existing.is_same(window)) {
            return true;
        }
    }
    map.set(window, mappingFunction(window));
    return false;
}
exports.computeWindowIfAbsent = computeWindowIfAbsent;
