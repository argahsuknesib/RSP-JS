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
exports.storeToString = exports.R2ROperator = void 0;
const rdf_data_factory_1 = require("rdf-data-factory");
const Logger_1 = require("../util/Logger");
const LOG_CONFIG = __importStar(require("../config/log_config.json"));
const rdfParser = require("rdf-parse").default;
const storeStream = require("rdf-store-stream").storeStream;
const streamifyString = require('streamify-string');
const n3_1 = require("n3");
const N3 = require('n3');
const DF = new rdf_data_factory_1.DataFactory();
class R2ROperator {
    constructor(query) {
        this.query = query;
        this.staticData = new Set();
        const log_level = Logger_1.LogLevel[LOG_CONFIG.log_level];
        this.logger = new Logger_1.Logger(log_level, LOG_CONFIG.classes_to_log, LOG_CONFIG.destination);
        this.logger.info("R2ROperator initialized with query: " + query, "R2ROperator");
    }
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
            this.logger.info("Executing R2R query on store with " + store.size + " quads", "R2ROperator");
            this.logger.info("Query: " + this.query, "R2ROperator");
            this.logger.info(`${storeToString(store)}`, "R2ROperator");
            const QueryEngine = require('@comunica/query-sparql').QueryEngine;
            const myEngine = new QueryEngine();
            return yield myEngine.queryBindings(this.query, {
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
