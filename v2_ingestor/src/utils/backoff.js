/**
 * Exponential backoff utility for handling rate limits and transient errors
 * Implements jittered exponential backoff with configurable retry strategies
 */

import { logger } from './logger.js';

export class BackoffStrategy {
  constructor(options = {}) {
    this.baseDelay = options.baseDelay || 1000; // 1 second
    this.maxDelay = options.maxDelay || 60000; // 60 seconds
    this.maxRetries = options.maxRetries || 5;
    this.factor = options.factor || 2;
    this.jitter = options.jitter !== false; // enabled by default
  }

  /**
   * Calculate delay for a given attempt with optional jitter
   */
  calculateDelay(attempt) {
    const exponentialDelay = Math.min(
      this.baseDelay * Math.pow(this.factor, attempt),
      this.maxDelay
    );

    if (!this.jitter) {
      return exponentialDelay;
    }

    // Add random jitter (±25% of delay)
    const jitterRange = exponentialDelay * 0.25;
    const jitter = Math.random() * jitterRange * 2 - jitterRange;
    return Math.max(0, exponentialDelay + jitter);
  }

  /**
   * Sleep for specified milliseconds
   */
  async sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

/**
 * Retry wrapper with exponential backoff
 */
export async function retryWithBackoff(fn, options = {}) {
  const strategy = new BackoffStrategy(options);
  const context = options.context || 'operation';
  const shouldRetry = options.shouldRetry || defaultShouldRetry;

  let lastError;

  for (let attempt = 0; attempt <= strategy.maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Check if we should retry this error
      if (!shouldRetry(error)) {
        logger.warn(`${context} failed with non-retryable error`, {
          error: error.message,
          attempt
        });
        throw error;
      }

      // Don't retry if we've exhausted attempts
      if (attempt >= strategy.maxRetries) {
        logger.error(`${context} failed after ${attempt + 1} attempts`, {
          error: error.message
        });
        throw error;
      }

      // Calculate and apply backoff
      const delay = strategy.calculateDelay(attempt);
      logger.warn(`${context} failed, retrying in ${Math.round(delay)}ms`, {
        attempt: attempt + 1,
        maxRetries: strategy.maxRetries,
        error: error.message,
        statusCode: error.response?.status
      });

      await strategy.sleep(delay);
    }
  }

  throw lastError;
}

/**
 * Default retry strategy: retry on network errors and 5xx/429 responses
 */
function defaultShouldRetry(error) {
  // Network errors
  if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || error.code === 'ENOTFOUND') {
    return true;
  }

  // HTTP status codes
  if (error.response) {
    const status = error.response.status;
    // Retry on rate limits and server errors
    if (status === 429 || status >= 500) {
      return true;
    }
    // Don't retry on client errors (except 429)
    if (status >= 400 && status < 500) {
      return false;
    }
  }

  // Default: retry on unknown errors
  return true;
}

/**
 * Rate limiter using token bucket algorithm
 */
export class RateLimiter {
  constructor(tokensPerSecond = 10, bucketSize = null) {
    this.tokensPerSecond = tokensPerSecond;
    this.bucketSize = bucketSize || tokensPerSecond * 2;
    this.tokens = this.bucketSize;
    this.lastRefill = Date.now();
  }

  /**
   * Refill tokens based on elapsed time
   */
  refill() {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    const tokensToAdd = elapsed * this.tokensPerSecond;

    this.tokens = Math.min(this.bucketSize, this.tokens + tokensToAdd);
    this.lastRefill = now;
  }

  /**
   * Wait until a token is available, then consume it
   */
  async acquire() {
    while (true) {
      this.refill();

      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }

      // Calculate wait time for next token
      const waitMs = (1 - this.tokens) / this.tokensPerSecond * 1000;
      await new Promise(resolve => setTimeout(resolve, Math.max(waitMs, 10)));
    }
  }

  /**
   * Check if tokens are available without consuming
   */
  available() {
    this.refill();
    return this.tokens >= 1;
  }
}

/**
 * Circuit breaker pattern for fault tolerance
 */
export class CircuitBreaker {
  constructor(options = {}) {
    this.failureThreshold = options.failureThreshold || 5;
    this.resetTimeout = options.resetTimeout || 60000; // 1 minute
    this.state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN
    this.failures = 0;
    this.nextAttempt = null;
  }

  /**
   * Execute function with circuit breaker protection
   */
  async execute(fn, context = 'operation') {
    if (this.state === 'OPEN') {
      if (Date.now() < this.nextAttempt) {
        throw new Error(`Circuit breaker OPEN for ${context}`);
      }
      // Try half-open state
      this.state = 'HALF_OPEN';
      logger.info(`Circuit breaker transitioning to HALF_OPEN for ${context}`);
    }

    try {
      const result = await fn();
      this.onSuccess(context);
      return result;
    } catch (error) {
      this.onFailure(context);
      throw error;
    }
  }

  onSuccess(context) {
    this.failures = 0;
    if (this.state === 'HALF_OPEN') {
      this.state = 'CLOSED';
      logger.info(`Circuit breaker CLOSED for ${context}`);
    }
  }

  onFailure(context) {
    this.failures += 1;

    if (this.failures >= this.failureThreshold) {
      this.state = 'OPEN';
      this.nextAttempt = Date.now() + this.resetTimeout;
      logger.error(`Circuit breaker OPEN for ${context}`, {
        failures: this.failures,
        resetAt: new Date(this.nextAttempt).toISOString()
      });
    }
  }

  reset() {
    this.state = 'CLOSED';
    this.failures = 0;
    this.nextAttempt = null;
  }
}

export default {
  BackoffStrategy,
  retryWithBackoff,
  RateLimiter,
  CircuitBreaker
};
