/// <reference types="node" />
import { EventEmitter } from "events";
import { Quad } from 'n3';
import { Logger } from "../util/Logger";
export declare enum ReportStrategy {
    NonEmptyContent = 0,
    OnContentChange = 1,
    OnWindowClose = 2,
    Periodic = 3,
    CountBased = 4
}
export declare enum Tick {
    TimeDriven = 0,
    TupleDriven = 1,
    BatchDriven = 2
}
/**
 * WindowInstance class to represent the window instance of the CSPARQL Window.
 */
export declare class WindowInstance {
    open: number;
    close: number;
    has_triggered: boolean;
    /**
     * Constructor for the WindowInstance class.
     * @param {number} open - The open time of the window instance of the form {open, close, has_triggered}.
     * @param {number} close - The close time of the window instance of the form {open, close, has_triggered}.
     */
    constructor(open: number, close: number);
    /**
     * Get the definition of the window instance.
     * @returns {string} - The definition of the window instance in the form [open, close) Triggered: has_triggered.
     */
    getDefinition(): string;
    /**
     * Get the code of the window instance.
     * @returns {number} - The code of the window instance.
     */
    hasCode(): number;
    /**
     * Check if the window instance is the same as the other window instance.
     * @param {WindowInstance} other_window - The other window instance to be compared.
     * @returns {boolean} - True if the window instances are the same, else false.
     */
    is_same(other_window: WindowInstance): boolean;
    set_triggered(): void;
}
/**
 * QuadContainer class to represent the container for the quads in the CSPARQL Window.
 */
export declare class QuadContainer {
    elements: Set<Quad>;
    last_time_stamp_changed: number;
    /**
     * Constructor for the QuadContainer class.
     * @param {Set<Quad>} elements - The set of quads in the container.
     * @param {number} ts - The timestamp of the last change in the container.
     */
    constructor(elements: Set<Quad>, ts: number);
    /**
     * Get the length of the container of the quads.
     * @returns {number} - The length of the container.
     */
    len(): number;
    /**
     * Add the quad to the container of the quads.
     * @param {Quad} quad - The quad to be added to the container.
     * @param {number} quad_timestamp - The timestamp of the quad.
     * @returns {void} - The function returns nothing.
     */
    add(quad: Quad, quad_timestamp: number): void;
    /**
     * Get the last time the container was changed.
     * @returns {number} - The last time the container was changed.
     */
    last_time_changed(): number;
}
/**
 * CSPARQL Window class that implements the windowing mechanism for the RSP Engine.
 * The class is responsible for managing the windows, processing the events, and emitting the triggers based on the report strategy.
 * The class also handles the out-of-order processing of the events based on the maximum delay allowed for the events and the watermark.
 */
export declare class CSPARQLWindow extends EventEmitter {
    width: number;
    slide: number;
    time: number;
    t0: number;
    active_windows: Map<WindowInstance, QuadContainer>;
    report: ReportStrategy;
    logger: Logger;
    tick: Tick;
    emitter: EventEmitter;
    name: string;
    private current_watermark;
    max_delay: number;
    pending_triggers: Set<WindowInstance>;
    private trigger_threshold;
    /**
     * Constructor for the CSPARQLWindow class.
     * @param {string} name - The name of the CSPARQL Window.
     * @param {number} width - The width of the window.
     * @param {number} slide - The slide of the window.
     * @param {ReportStrategy} report - The report strategy for the window.
     * @param {Tick} tick - The tick of the window.
     * @param {number} start_time - The start time of the window.
     * @param {number} max_delay - The maximum delay allowed for an observation to be considered in the window used for out-of-order processing.
     */
    constructor(name: string, width: number, slide: number, report: ReportStrategy, tick: Tick, start_time: number, max_delay: number, trigger_threshold: number);
    /**
     * Get the content of the window at the given timestamp if it exists, else return undefined.
     * @param {number} timestamp - The timestamp for which the content of the window is to be retrieved.
     * @returns {QuadContainer | undefined} - The content of the window if it exists, else undefined.
     */
    getContent(timestamp: number): QuadContainer | undefined;
    /**
     * Add the event to the window at the given timestamp and checks if the event is late or not.
     * @param {Quad} event - The event to be added to the window.
     * @param {number} timestamp - The timestamp of the event.
     * @returns {void} - The function does not return anything.
     */
    add(event: Quad, timestamp: number): void;
    if_event_late(timestamp: number): boolean;
    /**
     * Trigger the window content based on the current watermark.
     * @param {number} watermark - The current watermark which needs to be processed.
     * @returns {void} - The function does not return anything.
     */
    trigger_window_content(watermark: number, timestamp: number): void;
    private findWindowInstance;
    /**
     * Updating the watermark.
     * @param {number} new_time - The new watermark to be set.
     * @returns {void} - The function does not return anything.
     */
    update_watermark(new_time: number): void;
    /**
     * Get the current time of the window.
     * @returns {number} - The current time of the window.
     */
    get_current_watermark(): number;
    /**
     * Compute the report based on the window instance and the content of the window.
     * Max Delay is added to trigger the report computation only after waiting for a certain time.
     * @param {WindowInstance} w - The window instance for which the report is to be computed.
     * @param {QuadContainer} content - The content of the window (which is a QuadContainer).
     * @param {number} timestamp - The timestamp of the event to be processed.
     * @returns {boolean} - True if the report is to be computed, else false.
     */
    compute_report(w: WindowInstance, content: QuadContainer, timestamp: number): boolean;
    /**
     * Scope the window based on the given timestamp.
     * @param {number} t_e - The timestamp of the event to be processed.
     * @returns {void} - The function does not return anything.
     */
    scope(t_e: number): void;
    /**
     * Subscribe to the window based on the output stream and the callback function.
     * @param {'RStream' | 'IStream' | 'DStream'} output - The output stream to which the windvow is to be subscribed. The output stream can be one of {'RStream', 'IStream', 'DStream'}.
     * @param {(QuadContainer) => void} call_back - The callback function to be called when the window emits the triggers.
     * @returns {void} - The function does not return anything.
     */
    subscribe(output: 'RStream' | 'IStream' | 'DStream', call_back: (data: QuadContainer) => void): void;
    /**
     * Set the current time to the given value.
     * @param {number} t - The time to be set.
     * @returns {void} - The function does not return anything.
     */
    set_current_time(t: number): void;
    set_max_delay(delay: number): void;
    /**
     * Set the watermark to the given value.
     * @param {number} t - The watermark to be set.
     * @returns {void} - The function does not return anything.
     */
    set_current_watermark(t: number): void;
    /**
     * Get a string representation of the CSPARQLWindow definition.
     * The function is used to get the definition of the CSPARQLWindow in a string format.
     * @returns {string} - The string representation of the CSPARQLWindow definition.
     */
    getCSPARQLWindowDefinition(): string;
}
/**
 * Compute the window if absent based on the given window instance and the mapping function.
 * @param {Map<WindowInstance, QuadContainer>} map - The map of the active windows.
 * @param {WindowInstance} window - The window instance of the form {open, close, has_triggered}.
 * @param {mappingFunction} mappingFunction - The mapping function to be applied to the window instance.
 */
export declare function computeWindowIfAbsent(map: Map<WindowInstance, QuadContainer>, window: WindowInstance, mappingFunction: (key: WindowInstance) => QuadContainer): boolean;
