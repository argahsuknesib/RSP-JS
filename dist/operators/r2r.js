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
exports.stringToStore = exports.storeToString = exports.R2ROperator = void 0;
const rdf_data_factory_1 = require("rdf-data-factory");
const N3 = require('n3');
const n3_1 = require("n3");
const DF = new rdf_data_factory_1.DataFactory();
const rdfParser = require("rdf-parse").default;
const storeStream = require("rdf-store-stream").storeStream;
const streamifyString = require('streamify-string');
const QueryEngine = require('@comunica/query-sparql').QueryEngine;
const { EventEmitter } = require('events').EventEmitter;
let theEvent = new EventEmitter();
require('events').EventEmitter.defaultMaxListeners = Infinity;
const Logger_1 = require("../util/Logger");
const LoggerEnum_1 = require("../util/LoggerEnum");
const LOG_CONFIG = __importStar(require("../config/log_config.json"));
/**
 * R2R Operator Implementation Class for the RSP Engine.
 * It performs operations such as Join, Filter, Aggregation on a stream of data
 * to generate a new stream of data.
 */
class R2ROperator extends EventEmitter {
    constructor(query, rules) {
        super();
        this.rules = rules;
        this.query = query;
        const log_level = LoggerEnum_1.LogLevel[LOG_CONFIG.log_level];
        this.logger = new Logger_1.Logger(log_level, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination);
        this.logger.info(`R2ROperator initialized with query: ${query}`, 'R2ROperator');
        this.staticData = new Set();
    }
    /**
     * Add static data to the R2R Operator.
     * @param {Quad} quad - The quad to be added as static data.
     */
    addStaticData(quad) {
        this.staticData.add(quad);
    }
    execute(container) {
        return __awaiter(this, void 0, void 0, function* () {
            const store = new N3.Store();
            for (let elem of container.elements) {
                store.addQuad(elem);
            }
            for (let elem of this.staticData) {
                store.addQuad(elem);
            }
            this.logger.info(`Executing R2R Operator with query: ${this.query}`, 'R2ROperator');
            this.logger.info(`Static data size: ${this.staticData.size}`, 'R2ROperator');
            this.logger.info(`Store size: ${store.size}`, 'R2ROperator');
            this.logger.info(`Store content: ${storeToString(store).join('\n')}`, 'R2ROperator');
            const myEngine = new QueryEngine();
            const bindings_stream = myEngine.queryBindings(this.query, {
                sources: [store],
                extensionFunctions: {
                    'http://extension.org/functions#sqrt'(args) {
                        const arg = args[0];
                        if (arg.termType === 'Literal') {
                            return DF.literal(Math.sqrt(Number(arg.value)).toString());
                        }
                    },
                    'http://extension.org/functions#pow'(args) {
                        const arg1 = args[0];
                        if (arg1.termType === 'Literal') {
                            const arg2 = args[1];
                            if (arg2.termType === 'Literal') {
                                return DF.literal(Math.pow(Number(arg1.value), Number(arg2.value)).toString());
                            }
                        }
                    }
                },
            });
            return bindings_stream;
        });
    }
}
exports.R2ROperator = R2ROperator;
/**
 * Convert a store to a string representation.
 * @param {Store} store - The N3 Store to be converted.
 * @returns {string[]} - Array of string representations of the quads.
 */
function storeToString(store) {
    const writer = new n3_1.Writer();
    return store.getQuads(null, null, null, null).map(quad => writer.quadToString(quad.subject, quad.predicate, quad.object, quad.graph));
}
exports.storeToString = storeToString;
function stringToStore(text, options) {
    return __awaiter(this, void 0, void 0, function* () {
        const textStream = streamifyString(text);
        const quadStream = rdfParser.parse(textStream, options);
        return yield storeStream(quadStream);
    });
}
exports.stringToStore = stringToStore;
