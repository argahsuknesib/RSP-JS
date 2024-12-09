"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.LogDestination = exports.LogLevel = void 0;
var LogLevel;
(function (LogLevel) {
    LogLevel[LogLevel["TRACE"] = 0] = "TRACE";
    LogLevel[LogLevel["DEBUG"] = 1] = "DEBUG";
    LogLevel[LogLevel["INFO"] = 2] = "INFO";
    LogLevel[LogLevel["CONFIG"] = 3] = "CONFIG";
    LogLevel[LogLevel["WARN"] = 4] = "WARN";
    LogLevel[LogLevel["ERROR"] = 5] = "ERROR";
    LogLevel[LogLevel["FATAL"] = 6] = "FATAL";
    LogLevel[LogLevel["SEVERE"] = 7] = "SEVERE";
    LogLevel[LogLevel["AUDIT"] = 8] = "AUDIT";
    LogLevel[LogLevel["STATS"] = 9] = "STATS";
})(LogLevel = exports.LogLevel || (exports.LogLevel = {}));
var LogDestination;
(function (LogDestination) {
    LogDestination[LogDestination["CONSOLE"] = 0] = "CONSOLE";
    LogDestination[LogDestination["FILE"] = 1] = "FILE";
})(LogDestination = exports.LogDestination || (exports.LogDestination = {}));
