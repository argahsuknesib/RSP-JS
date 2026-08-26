import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { Logger } from './Logger';
import { LogDestination, LogLevel } from './LoggerEnum';

const originalDisabled = process.env.RSP_JS_DISABLE_LOGGING;
const originalCwd = process.cwd();
afterEach(() => {
    if (originalDisabled === undefined) delete process.env.RSP_JS_DISABLE_LOGGING;
    else process.env.RSP_JS_DISABLE_LOGGING = originalDisabled;
    process.chdir(originalCwd);
    jest.restoreAllMocks();
});

test('keeps diagnostic logging enabled when the benchmark flag is absent', () => {
    delete process.env.RSP_JS_DISABLE_LOGGING;
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'rsp-logger-'));
    fs.mkdirSync(path.join(directory, 'logs'));
    process.chdir(directory);
    const consoleLog = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    const logger = new Logger(LogLevel.INFO, ['Test'], 'FILE');
    logger.info('message', 'Test');
    expect(consoleLog).toHaveBeenCalledWith(expect.stringContaining('Logger initialized'));
    expect(fs.readFileSync(path.join(directory, 'logs', 'Test.log'), 'utf8')).toContain('message');
});

test('suppresses diagnostic console and file logging when benchmark logging is disabled', () => {
    process.env.RSP_JS_DISABLE_LOGGING = '1';
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'rsp-logger-'));
    process.chdir(directory);
    const consoleLog = jest.spyOn(console, 'log').mockImplementation(() => undefined);
    const consoleError = jest.spyOn(console, 'error').mockImplementation(() => undefined);
    const logger = new Logger(LogLevel.INFO, ['Test'], 'FILE');
    logger.info('message', 'Test');
    expect(consoleLog).not.toHaveBeenCalled();
    expect(consoleError).not.toHaveBeenCalled();
    expect(fs.existsSync(path.join(directory, 'logs', 'Test.log'))).toBe(false);
});
