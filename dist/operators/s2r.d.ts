/// <reference types="node" />
import { EventEmitter } from "events";
import { Quad } from "n3";
import { Logger } from "../util/Logger";
export declare enum ReportStrategy {
    NonEmptyContent = 0,
    OnContentChange = 1,
    OnWindowClose = 2,
    Periodic = 3
}
export declare enum Tick {
    TimeDriven = 0,
    TupleDriven = 1,
    BatchDriven = 2
}
/** The timestamp used to describe a completed window in an output result. */
export declare enum WindowSemantics {
    Trailing = "trailing",
    Centered = "centered"
}
export type OutOfOrderObservation = {
    event_time_ms: number;
    reference_time_ms: number;
    out_of_order: boolean;
    lateness_ms: number;
    max_out_of_orderness_ms: number;
    within_bound: boolean;
};
export declare class WindowInstance {
    open: number;
    close: number;
    has_triggered: boolean;
    constructor(open: number, close: number);
    getDefinition(): string;
    hasCode(): number;
    is_same(other_window: WindowInstance): boolean;
    set_triggered(): void;
}
export declare class QuadContainer {
    elements: Set<Quad>;
    last_time_stamp_changed: number;
    window_start?: number;
    window_end?: number;
    logical_trigger_time?: number;
    window_semantics?: WindowSemantics;
    constructor(elements: Set<Quad>, ts: number);
    len(): number;
    add(quad: Quad, quad_timestamp: number): void;
    last_time_changed(): number;
}
export declare class CSPARQLWindow {
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
    private current_watermark;
    max_delay: number;
    window_semantics: WindowSemantics;
    pending_triggers: Set<WindowInstance>;
    private scope_origin_initialized;
    constructor(name: string, width: number, slide: number, report: ReportStrategy, tick: Tick, start_time: number, max_delay?: number, window_semantics?: WindowSemantics);
    /** Return the active window with the earliest close that contains a timestamp. */
    getContent(timestamp: number): QuadContainer | undefined;
    /** Add one event or a set of events to all matching windows. */
    add(event: Quad | Set<Quad>, timestamp: number): OutOfOrderObservation;
    if_event_late(timestamp: number): boolean;
    compute_report(window: WindowInstance, _content: QuadContainer, watermark: number): boolean;
    /** Emit and retire every window whose close is covered by the watermark. */
    trigger_window_content(watermark: number): void;
    update_watermark(new_time: number): void;
    get_current_watermark(): number;
    scope(timestamp: number): void;
    private getLogicalTriggerTime;
    subscribe(output: "RStream" | "IStream" | "DStream", call_back: (data: QuadContainer) => void): void;
    set_current_time(time: number): void;
    set_max_delay(delay: number): void;
    set_current_watermark(time: number): void;
    getCSPARQLWindowDefinition(): string;
}
export declare function computeWindowIfAbsent(map: Map<WindowInstance, QuadContainer>, window: WindowInstance, mappingFunction: (key: WindowInstance) => QuadContainer): boolean;
