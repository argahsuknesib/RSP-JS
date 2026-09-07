import { EventEmitter } from "events";
// @ts-ignore: n3 does not expose the Quad type consistently across versions.
import { Quad } from "n3";
import * as LOG_CONFIG from "../config/log_config.json";
import { LogDestination, LogLevel, Logger } from "../util/Logger";

export enum ReportStrategy {
    NonEmptyContent,
    OnContentChange,
    OnWindowClose,
    Periodic,
}

export enum Tick {
    TimeDriven,
    TupleDriven,
    BatchDriven,
}

/** The timestamp used to describe a completed window in an output result. */
export enum WindowSemantics {
    Trailing = "trailing",
    Centered = "centered",
}

export type OutOfOrderObservation = {
    event_time_ms: number,
    reference_time_ms: number,
    out_of_order: boolean,
    lateness_ms: number,
    max_out_of_orderness_ms: number,
    within_bound: boolean,
};

export class WindowInstance {
    open: number;
    close: number;

    constructor(open: number, close: number) {
        this.open = open;
        this.close = close;
    }

    getDefinition() {
        return `[${this.open},${this.close})`;
    }

    hasCode() {
        return 0;
    }

    is_same(other_window: WindowInstance): boolean {
        return this.open === other_window.open && this.close === other_window.close;
    }
}

export class QuadContainer {
    elements: Set<Quad>;
    last_time_stamp_changed: number;
    window_start?: number;
    window_end?: number;
    logical_trigger_time?: number;
    window_semantics?: WindowSemantics;

    constructor(elements: Set<Quad>, ts: number, window_start?: number, window_end?: number) {
        this.elements = elements;
        this.last_time_stamp_changed = ts;
        this.window_start = window_start;
        this.window_end = window_end;
    }

    len() {
        return this.elements.size;
    }

    add(quad: Quad, quad_timestamp: number): boolean {
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

export class CSPARQLWindow {
    width: number;
    slide: number;
    time: number;
    /** The origin used to align generated windows. */
    t0: number;
    active_windows: Map<WindowInstance, QuadContainer>;
    report: ReportStrategy;
    tick: Tick;
    emitter: EventEmitter;
    name: string;
    logger: Logger;
    private current_watermark: number;
    public max_delay: number;
    public window_semantics: WindowSemantics;
    private pending_triggers: Set<WindowInstance>;

    constructor(
        name: string,
        width: number,
        slide: number,
        report: ReportStrategy,
        tick: Tick,
        start_time: number,
        max_delay = 0,
        window_semantics: WindowSemantics = WindowSemantics.Trailing,
    ) {
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
        this.active_windows = new Map<WindowInstance, QuadContainer>();
        this.emitter = new EventEmitter();
        this.pending_triggers = new Set<WindowInstance>();

        const logLevel = LogLevel[LOG_CONFIG.log_level as keyof typeof LogLevel];
        this.logger = new Logger(
            logLevel,
            LOG_CONFIG.classes_to_log,
            LOG_CONFIG.destination as unknown as LogDestination,
        );
    }

    /** Return the active window with the earliest close that contains a timestamp. */
    getContent(timestamp: number): QuadContainer | undefined {
        let selected: WindowInstance | undefined;
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
    getContentForWindow(window_start: number, window_end: number): QuadContainer | undefined {
        for (const [window, content] of this.active_windows.entries()) {
            if (window.open === window_start && window.close === window_end) {
                return content;
            }
        }
        return undefined;
    }

    /** Add one event or a set of events to all matching windows. */
    add(event: Quad | Set<Quad>, timestamp: number): OutOfOrderObservation {
        const referenceTime = this.time;
        const outOfOrder = timestamp < referenceTime;
        const lateness = outOfOrder ? referenceTime - timestamp : 0;
        const withinBound = !outOfOrder || lateness <= this.max_delay;
        const observation: OutOfOrderObservation = {
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
        } else {
            this.time = timestamp;
            this.logger.debug(`Accepting in-order event at ${timestamp}`, "CSPARQLWindow");
            this.scope(timestamp);
        }

        const quads = event instanceof Set ? event : new Set<Quad>([event]);
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

    if_event_late(timestamp: number) {
        return timestamp < this.time;
    }

    compute_report(window: WindowInstance, _content: QuadContainer, watermark: number): boolean {
        if (this.report === ReportStrategy.OnWindowClose) {
            return window.close <= watermark;
        }
        if (this.report === ReportStrategy.OnContentChange) {
            return this.pending_triggers.has(window) && _content.len() > 0;
        }
        return false;
    }

    /** Emit changed content and retire windows whose close is covered by the watermark. */
    trigger_window_content(watermark: number): void {
        const windowsToRetire: WindowInstance[] = [];

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

    private annotateAndEmit(window: WindowInstance, content: QuadContainer): void {
        content.window_start = window.open;
        content.window_end = window.close;
        content.logical_trigger_time = this.getLogicalTriggerTime(window);
        content.window_semantics = this.window_semantics;
        this.emitter.emit("RStream", content);
    }

    update_watermark(new_time: number): void {
        if (new_time <= this.current_watermark) {
            return;
        }

        this.current_watermark = new_time;
        this.trigger_window_content(this.current_watermark);
    }

    get_current_watermark() {
        return this.current_watermark;
    }

    scope(timestamp: number) {
        const firstAlignedStart = Math.floor((timestamp - this.t0) / this.slide) * this.slide + this.t0;
        let windowStart = firstAlignedStart - this.width;

        while (windowStart <= timestamp) {
            computeWindowIfAbsent(
                this.active_windows,
                new WindowInstance(windowStart, windowStart + this.width),
                (window) => new QuadContainer(new Set<Quad>(), 0, window.open, window.close),
            );
            windowStart += this.slide;
        }
    }

    private getLogicalTriggerTime(window: WindowInstance): number {
        if (this.window_semantics === WindowSemantics.Centered) {
            return window.open + Math.floor(this.width / 2);
        }
        return window.close;
    }

    subscribe(output: "RStream" | "IStream" | "DStream", call_back: (data: QuadContainer) => void) {
        this.emitter.on(output, call_back);
    }

    set_current_time(time: number) {
        this.time = Math.max(this.time, time);
    }

    set_max_delay(delay: number) {
        this.max_delay = Math.max(0, delay);
    }

    set_current_watermark(time: number) {
        this.update_watermark(time);
    }

    getCSPARQLWindowDefinition() {
        const windowDefinitions: string[] = [];
        for (const window of this.active_windows.keys()) {
            windowDefinitions.push(window.getDefinition());
        }
        return `CSPARQLWindow { name: ${this.name}, width: ${this.width}, slide: ${this.slide}, current_time: ${this.time}, current_watermark: ${this.current_watermark}, active_windows: [${windowDefinitions.join(", ")}] }`;
    }
}

export function computeWindowIfAbsent(
    map: Map<WindowInstance, QuadContainer>,
    window: WindowInstance,
    mappingFunction: (key: WindowInstance) => QuadContainer,
): boolean {
    for (const existing of map.keys()) {
        if (existing.is_same(window)) {
            return true;
        }
    }

    map.set(window, mappingFunction(window));
    return false;
}
