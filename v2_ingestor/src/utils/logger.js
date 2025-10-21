/**
 * Centralized logging utility with structured output and safe redaction
 * Supports multiple log levels and JSON formatting for Cloud Logging
 */

const LOG_LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
  fatal: 4
};

class Logger {
  constructor(context = 'app') {
    this.context = context;
    this.level = process.env.LOG_LEVEL?.toLowerCase() || 'info';
    this.minLevel = LOG_LEVELS[this.level] || LOG_LEVELS.info;
  }

  /**
   * Redact sensitive information from log messages
   */
  redact(data) {
    if (typeof data !== 'object' || data === null) {
      return data;
    }

    const redacted = Array.isArray(data) ? [...data] : { ...data };
    const sensitiveKeys = ['password', 'token', 'api_key', 'apiKey', 'secret', 'authorization', 'clientSecret'];

    for (const key in redacted) {
      if (sensitiveKeys.some(sk => key.toLowerCase().includes(sk))) {
        redacted[key] = '***REDACTED***';
      } else if (typeof redacted[key] === 'object' && redacted[key] !== null) {
        redacted[key] = this.redact(redacted[key]);
      }
    }

    return redacted;
  }

  /**
   * Format log entry for Cloud Logging compatibility
   */
  format(level, message, meta = {}) {
    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp,
      severity: level.toUpperCase(),
      context: this.context,
      message,
      ...this.redact(meta)
    };

    return JSON.stringify(logEntry);
  }

  /**
   * Core logging method
   */
  log(level, message, meta = {}) {
    if (LOG_LEVELS[level] < this.minLevel) {
      return;
    }

    const formatted = this.format(level, message, meta);

    if (level === 'error' || level === 'fatal') {
      console.error(formatted);
    } else {
      console.log(formatted);
    }
  }

  debug(message, meta) {
    this.log('debug', message, meta);
  }

  info(message, meta) {
    this.log('info', message, meta);
  }

  warn(message, meta) {
    this.log('warn', message, meta);
  }

  error(message, meta) {
    this.log('error', message, meta);
  }

  fatal(message, meta) {
    this.log('fatal', message, meta);
  }

  /**
   * Create a child logger with extended context
   */
  child(additionalContext) {
    const childLogger = new Logger(`${this.context}:${additionalContext}`);
    childLogger.level = this.level;
    childLogger.minLevel = this.minLevel;
    return childLogger;
  }

  /**
   * Log execution time of async operations
   */
  async time(label, fn) {
    const start = Date.now();
    this.info(`${label} started`);

    try {
      const result = await fn();
      const duration = Date.now() - start;
      this.info(`${label} completed`, { duration_ms: duration });
      return result;
    } catch (error) {
      const duration = Date.now() - start;
      this.error(`${label} failed`, { duration_ms: duration, error: error.message });
      throw error;
    }
  }
}

// Export singleton instance and class
export const logger = new Logger('v2-ingestor');
export default Logger;
