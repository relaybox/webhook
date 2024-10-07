import winston, { format } from 'winston';
import { colorize } from 'json-colorizer';

const { printf, combine, timestamp, errors, colorize: winstonColorize } = format;

enum LogLevel {
  LOG = 'log',
  INFO = 'info',
  WARN = 'warn',
  DEBUG = 'debug'
}

const easyLogFormat = printf((info) => {
  const { level, service, message } = info;

  let baseLog = `[${level}]:${new Date().toISOString().slice(11)} ${service} - ${message}`;
  // .slice(11)} ${service} - ${message}\n${colorize(JSON.stringify(info, null, 2))}\n`;

  if (info.err && info.err.stack) {
    baseLog += `\n${info.err.stack}`;
  }

  return baseLog;
});

const customLogFormat = printf((info) => {
  const { level, ...rest } = info;

  const logDetails = JSON.stringify(rest, null, 4);

  return `[${level}]: ${logDetails}`;
});

const prettyPrint = new winston.transports.Console({
  level: process.env.LOG_LEVEL || LogLevel.INFO,
  format: combine(timestamp(), winstonColorize(), customLogFormat)
});

const easyPrint = new winston.transports.Console({
  level: process.env.LOG_LEVEL || LogLevel.INFO,
  format: combine(errors({ stack: true }), timestamp(), winstonColorize(), easyLogFormat)
});

const flatPrint = new winston.transports.Console({
  level: process.env.LOG_LEVEL || LogLevel.INFO,
  format: combine(
    timestamp(),
    errors({ stack: true }) // Ensures stack trace is captured
  )
});

const transports = process.env.LOCALHOST === 'true' ? [easyPrint] : [flatPrint];
// const transports = process.env.LOCALHOST === 'true' ? [prettyPrint] : [flatPrint];

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || LogLevel.INFO,
  transports
});

// logger.transports[0].silent = true;

export function getLogger(service: string) {
  return logger.child({ service });
}
