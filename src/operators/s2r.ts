import { EventEmitter } from "events";
// @ts-ignore
import { Quad } from 'n3';
import { LogDestination, Logger, LogLevel } from "../util/Logger";
import * as LOG_CONFIG from "../config/log_config.json";

export enum ReportStrategy {
    NonEmptyContent,
    OnContentChange,
    OnWindowClose,
    Periodic
}
export enum Tick {
    TimeDriven,
    TupleDriven,
    BatchDriven,
}

export class WindowInstance {
    open: number;
    close: number;
    constructor(open: number, close: number) {
        this.open = open;
        this.close = close;
    }

    getDefinition() {
        return "[" + this.open + "," + this.close + ")";
    }
    hasCode() {
        return 0;
    }
}


export class QuadContainer {
    elements: Set<Quad>;
    last_time_stamp_changed: number;
    constructor(elements: Set<Quad>, ts: number) {
        this.elements = elements;
        this.last_time_stamp_changed = ts;
    }

    len() {
        return this.elements.size;
    }
    add(quad: Quad, ts: number) {
        this.elements.add(quad);
        this.last_time_stamp_changed = ts;
    }

    last_time_changed() {
        return this.last_time_stamp_changed;
    }

}

export class CSPARQLWindow {
    width: number;
    slide: number;
    time: number;
    t0: number;
    active_windows: Map<WindowInstance, QuadContainer>;
    report: ReportStrategy;
    tick: Tick;
    emitter: EventEmitter;
    name: string;
    logger: Logger;
    constructor(name: string, width: number, slide: number, report: ReportStrategy, tick: Tick, start_time: number) {
        this.name = name;
        this.width = width;
        this.slide = slide;
        this.report = report;
        const logLevel: LogLevel = LogLevel[LOG_CONFIG.log_level as keyof typeof LogLevel];
        this.logger = new Logger(logLevel, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination as unknown as LogDestination);
        this.tick = tick;
        this.time = start_time;
        this.t0 = start_time;
        this.active_windows = new Map<WindowInstance, QuadContainer>();
        let EventEmitter = require('events').EventEmitter;
        this.emitter = new EventEmitter();
    }
    getContent(timestamp: number): QuadContainer | undefined {
        let max_window: WindowInstance | null = null;
        let max_time = Number.MAX_SAFE_INTEGER;
        this.active_windows.forEach((value: QuadContainer, window: WindowInstance) => {
            if (window.open <= timestamp && timestamp <= window.close) {
                if (window.close < max_time) {
                    max_time = window.close;
                    max_window = window;
                }
            }
        });
        if (max_window) {
            return this.active_windows.get(max_window);
        } else {
            return undefined;
        }
    }

    add(e: Quad, timestamp: number) {
        this.logger.debug(`Window ${this.name} Received element (${e} , ${timestamp})`, `CSPARQLWindow`);
        let toEvict = new Set<WindowInstance>();
        let t_e = timestamp;

        if (this.time > t_e) {
            this.logger.error("OUT OF ORDER NOT HANDLED", `CSPARQLWindow`);
        }

        this.scope(t_e);

        for (let w of this.active_windows.keys()) {
            this.logger.debug(`Processing Window ${this.name} [${w.open},${w.close}) for element (${e},${timestamp})`, `CSPARQLWindow`);
            if (w.open <= t_e && t_e < w.close) {
                this.logger.debug(`Adding element (${e},${timestamp}) to Window ${this.name} [${w.open},${w.close})`, `CSPARQLWindow`);
                let temp_window = this.active_windows.get(w);
                if (temp_window) {
                    temp_window.add(e, timestamp);
                }
            } else if (t_e > w.close) {
                this.logger.debug(`Scheduling Window ${this.name} [${w.open},${w.close}) for eviction`, `CSPARQLWindow`);
                toEvict.add(w);
            }
        }
        let max_window: WindowInstance | null = null;
        let max_time = 0;
        this.active_windows.forEach((value: QuadContainer, window: WindowInstance) => {
            if (this.compute_report(window, value, timestamp)) {
                if (window.close > max_time) {
                    max_time = window.close;
                    max_window = window;
                }
            }
        });



        if (max_window) {
            const activeWindow = this.active_windows.get(max_window);
            if (activeWindow !== undefined) {
                if (activeWindow.len() > 0) {
                    this.logger.info(`Max Window ${this.name} [${JSON.stringify(max_window)})`, `CSPARQLWindow`);
                }
            }
        }

        if (this.tick == Tick.TimeDriven) {
            if (max_window !== null && max_window !== undefined) {
                if (timestamp > this.time) {
                    this.time = timestamp;
                    this.emitter.emit('RStream', this.active_windows.get(max_window));
                    // @ts-ignore
                    if (this.active_windows.get(max_window).len() > 0) {
                        // @ts-ignore
                        this.logger.info("Window [" + max_window.open + "," + max_window.close + ") triggers. Content Size: " + this.active_windows.get(max_window)?.len(), `CSPARQLWindow`);
                    } // this.logger.info("Window [" + max_window.open + "," + max_window.close + ") triggers. Content: " + this.active_windows.get(max_window), `CSPARQLWindow`);
                }
            }
            else {
                this.logger.debug("No window to trigger", `CSPARQLWindow`);
            }
        }

        for (let w of toEvict) {
            this.logger.debug("Evicting [" + w.open + "," + w.close + ")", `CSPARQLWindow`);
            this.active_windows.delete(w);
        }


    }
    compute_report(w: WindowInstance, content: QuadContainer, timestamp: number) {
        if (this.report == ReportStrategy.OnWindowClose) {
            return w.close < timestamp;
        }
        return false;

    }

    scope(t_e: number) {
        let c_sup = Math.ceil((Math.abs(t_e - this.t0) / this.slide)) * this.slide;
        let o_i = c_sup - this.width;
        this.logger.debug(`Calculating the Windows to Open. First one opens at [${o_i}] and closes at [${c_sup}]`, `CSPARQLWindow`);
        do {
            this.logger.debug("Computing Window [" + o_i + "," + (o_i + this.width) + ") if absent", `CSPARQLWindow`);
            computeWindowIfAbsent(this.active_windows, new WindowInstance(o_i, o_i + this.width), () => new QuadContainer(new Set<Quad>(), 0));
            o_i += this.slide;

        } while (o_i <= t_e);

    }

    subscribe(output: 'RStream' | 'IStream' | 'DStream', call_back: (data: QuadContainer) => void) {
        this.emitter.on(output, call_back);
    }
}
function computeWindowIfAbsent(map: Map<WindowInstance, QuadContainer>, key: WindowInstance, mappingFunction: (key: WindowInstance) => QuadContainer) {
    let val = map.get(key);
    let found = false;
    for (let w of map.keys()) {
        if (w.open == key.open && w.close == key.close) {
            found = true;
            break;
        }
    }
    if (!found) {
        val = mappingFunction(key);
        map.set(key, val);
    }

}
