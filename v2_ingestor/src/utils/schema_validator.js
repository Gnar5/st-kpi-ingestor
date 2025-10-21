/**
 * Schema validation and evolution tracking
 * Validates API responses against expected schema and logs drift
 */

import { logger } from './logger.js';
import { readFileSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export class SchemaValidator {
  constructor(schemaRegistryPath = null) {
    this.schemaRegistryPath = schemaRegistryPath || join(__dirname, '../../schema_registry.json');
    this.schemas = this.loadSchemas();
    this.driftDetected = new Set();
  }

  /**
   * Load schema registry from disk
   */
  loadSchemas() {
    try {
      const data = readFileSync(this.schemaRegistryPath, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      logger.warn('Schema registry not found, initializing empty registry', {
        path: this.schemaRegistryPath,
        error: error.message
      });
      return { version: '2.0.0', entities: {} };
    }
  }

  /**
   * Save schema registry to disk
   */
  saveSchemas() {
    try {
      const data = JSON.stringify(this.schemas, null, 2);
      writeFileSync(this.schemaRegistryPath, data, 'utf8');
      logger.info('Schema registry updated', { path: this.schemaRegistryPath });
    } catch (error) {
      logger.error('Failed to save schema registry', {
        path: this.schemaRegistryPath,
        error: error.message
      });
    }
  }

  /**
   * Get schema for entity type
   */
  getSchema(entityType) {
    return this.schemas.entities[entityType];
  }

  /**
   * Register or update schema for entity type
   */
  registerSchema(entityType, fields, options = {}) {
    const schema = {
      fields,
      version: options.version || '1.0.0',
      lastUpdated: new Date().toISOString(),
      description: options.description || '',
      primaryKey: options.primaryKey || 'id',
      partitionField: options.partitionField || null,
      clusterFields: options.clusterFields || []
    };

    this.schemas.entities[entityType] = schema;
    this.saveSchemas();

    logger.info('Schema registered', { entityType, fieldCount: Object.keys(fields).length });
    return schema;
  }

  /**
   * Infer schema from sample data
   */
  inferSchema(data) {
    if (!data || typeof data !== 'object') {
      return {};
    }

    const schema = {};

    for (const [key, value] of Object.entries(data)) {
      schema[key] = {
        type: this.inferType(value),
        nullable: value === null,
        discovered: new Date().toISOString()
      };
    }

    return schema;
  }

  /**
   * Infer BigQuery type from value
   */
  inferType(value) {
    if (value === null || value === undefined) {
      return 'STRING'; // Default for null
    }

    if (typeof value === 'boolean') {
      return 'BOOL';
    }

    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'INT64' : 'FLOAT64';
    }

    if (typeof value === 'string') {
      // Check for timestamp patterns
      if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
        return 'TIMESTAMP';
      }
      // Check for date patterns
      if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
        return 'DATE';
      }
      return 'STRING';
    }

    if (Array.isArray(value)) {
      if (value.length === 0) {
        return 'ARRAY<STRING>';
      }
      const elementType = this.inferType(value[0]);
      return `ARRAY<${elementType}>`;
    }

    if (typeof value === 'object') {
      return 'JSON';
    }

    return 'STRING';
  }

  /**
   * Validate data against registered schema
   */
  validate(entityType, data) {
    const schema = this.getSchema(entityType);

    if (!schema) {
      logger.debug('No schema registered for entity type', { entityType });
      return {
        valid: true,
        warnings: [`No schema registered for ${entityType}`],
        errors: []
      };
    }

    const warnings = [];
    const errors = [];
    const dataFields = new Set(Object.keys(data));
    const schemaFields = new Set(Object.keys(schema.fields));

    // Check for missing fields
    for (const field of schemaFields) {
      if (!dataFields.has(field) && !schema.fields[field].nullable) {
        errors.push(`Required field missing: ${field}`);
      }
    }

    // Check for unknown fields (schema drift)
    for (const field of dataFields) {
      if (!schemaFields.has(field)) {
        warnings.push(`Unknown field detected: ${field}`);
        this.trackDrift(entityType, field, data[field]);
      }
    }

    // Type validation
    for (const [field, value] of Object.entries(data)) {
      if (schemaFields.has(field)) {
        const expectedType = schema.fields[field].type;
        const actualType = this.inferType(value);

        if (expectedType !== actualType && value !== null) {
          warnings.push(`Type mismatch for ${field}: expected ${expectedType}, got ${actualType}`);
        }
      }
    }

    const valid = errors.length === 0;

    if (!valid) {
      logger.error('Schema validation failed', { entityType, errors });
    } else if (warnings.length > 0) {
      logger.warn('Schema validation warnings', { entityType, warnings });
    }

    return { valid, warnings, errors };
  }

  /**
   * Track schema drift for analysis
   */
  trackDrift(entityType, field, value) {
    const driftKey = `${entityType}.${field}`;

    if (!this.driftDetected.has(driftKey)) {
      this.driftDetected.add(driftKey);

      logger.warn('Schema drift detected', {
        entityType,
        field,
        type: this.inferType(value),
        sample: typeof value === 'object' ? JSON.stringify(value).substring(0, 100) : value
      });

      // Auto-update schema with new field
      const schema = this.getSchema(entityType);
      if (schema) {
        schema.fields[field] = {
          type: this.inferType(value),
          nullable: true,
          discovered: new Date().toISOString(),
          source: 'auto-detected'
        };
        this.saveSchemas();
      }
    }
  }

  /**
   * Normalize data to match schema types
   */
  normalize(entityType, data) {
    const schema = this.getSchema(entityType);

    if (!schema) {
      return data;
    }

    const normalized = { ...data };

    for (const [field, fieldSchema] of Object.entries(schema.fields)) {
      if (field in normalized) {
        normalized[field] = this.coerceType(normalized[field], fieldSchema.type);
      }
    }

    return normalized;
  }

  /**
   * Coerce value to expected type
   */
  coerceType(value, targetType) {
    if (value === null || value === undefined) {
      return null;
    }

    try {
      switch (targetType) {
        case 'INT64':
          return typeof value === 'number' ? Math.floor(value) : parseInt(value, 10);
        case 'FLOAT64':
          return typeof value === 'number' ? value : parseFloat(value);
        case 'BOOL':
          return typeof value === 'boolean' ? value : Boolean(value);
        case 'TIMESTAMP':
        case 'DATE':
          return value instanceof Date ? value.toISOString() : new Date(value).toISOString();
        case 'JSON':
          return typeof value === 'string' ? JSON.parse(value) : JSON.stringify(value);
        case 'STRING':
        default:
          return String(value);
      }
    } catch (error) {
      logger.warn('Type coercion failed', { value, targetType, error: error.message });
      return value;
    }
  }

  /**
   * Generate BigQuery schema from registry
   */
  toBigQuerySchema(entityType) {
    const schema = this.getSchema(entityType);

    if (!schema) {
      throw new Error(`No schema registered for entity type: ${entityType}`);
    }

    return Object.entries(schema.fields).map(([name, field]) => ({
      name,
      type: field.type.replace('ARRAY<', '').replace('>', ''),
      mode: field.type.startsWith('ARRAY') ? 'REPEATED' : (field.nullable ? 'NULLABLE' : 'REQUIRED')
    }));
  }
}

export default SchemaValidator;
