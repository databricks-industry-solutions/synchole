import { z } from 'zod';
import { checkJobSource, librarySchema, notebooksSchema, withLegacySource } from './jobSource.js';

export const identifierPart = z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/);
const coercedBoolean = z.preprocess((val) => (typeof val === 'string' ? val === 'true' : val), z.boolean());
// An empty field means "not set".
const optionalText = (schema: z.ZodString) =>
  z.preprocess((val) => (typeof val === 'string' && !val.trim() ? null : val), schema.nullable().optional());
// Same rules as the Python worker (synchole/change_tracking.py): these names are put into SQL.
const objectName = /^[A-Za-z_][\w$#]*(\.[A-Za-z_][\w$#]*){0,2}$/;
const columnName = /^[A-Za-z_][\w$#]*$/;
const columnNames = /^[A-Za-z_][\w$#]*(\s*,\s*[A-Za-z_][\w$#]*)*$/;
export const changeTrackingMethods = ['watermark', 'change_table', 'change_procedure'] as const;
export const profileMethods = ['change_table', 'change_procedure'] as const;
const usesChangeSource = (tracking: string) => tracking === 'change_table' || tracking === 'change_procedure';
// Placeholders of change_table_name and results_table, and the ones procedure_call adds.
export const namePlaceholders = ['schema', 'table', 'table_lower'] as const;
export const procedurePlaceholders = [
  ...namePlaceholders,
  'start',
  'end',
  'temp_table',
  'multi_key',
  'skip_change_time',
  'columns',
] as const;
const placeholdersOf = (template: string) => new Set([...template.matchAll(/\{([A-Za-z_]+)\}/g)].map((m) => m[1]));
const unknownPlaceholders = (template: string, allowed: readonly string[]) =>
  [...placeholdersOf(template)].filter((name) => !allowed.includes(name));
export const locationSchema = z.object({ catalog: identifierPart, schema: identifierPart });
export const configSchema = locationSchema
  .extend({
    originalIngestionGroup: z.string().optional(),
    originalSourceTableName: z.string().optional(),
    ingestionGroup: z.string().min(1),
    sourceTableName: z.string().min(1),
    stagingTableFqn: z.string().nullable().optional(),
    targetTableFqn: z.string().regex(/^[^.]+\.[^.]+\.[^.]+$/),
    ingestionType: z.enum(['incremental', 'full']),
    keyColumns: z.string().nullable().optional(),
    watermarkColumn: z.string().nullable().optional(),
    partitionColumn: z.string().nullable().optional(),
    predicateColumn: z.string().nullable().optional(),
    changeTracking: z
      .enum(changeTrackingMethods)
      .nullish()
      .transform((value) => value ?? 'watermark'),
    changeProfile: optionalText(z.string().trim()),
    changeColumns: optionalText(z.string().trim().regex(columnNames, 'Use comma-separated column names')),
    skipChangeTime: coercedBoolean.optional().default(false),
    autoCdcFromSnapshot: coercedBoolean,
    jdbcUrl: z.string().nullable().optional(),
    jdbcUser: z.string().nullable().optional(),
    jdbcSecretScope: z.string().nullable().optional(),
    jdbcSecretKey: z.string().nullable().optional(),
    ucSecretName: z.string().nullable().optional(),
    connectionName: z.string().nullable().optional(),
    connectionDatabase: z.string().nullable().optional(),
    watermarkThresholdMinutes: z.number().int().nonnegative(),
    fetchSize: z.number().int().positive(),
    numPartitions: z.number().int().positive(),
    enabled: coercedBoolean,
  })
  .superRefine((value, context) => {
    const changeSource = usesChangeSource(value.changeTracking);
    if (changeSource && value.ingestionType !== 'incremental') {
      context.addIssue({
        code: 'custom',
        path: ['changeTracking'],
        message: 'A change table or change procedure requires incremental ingestion',
      });
    }
    if (changeSource && !value.changeProfile) {
      context.addIssue({ code: 'custom', path: ['changeProfile'], message: 'Choose a change-tracking profile' });
    }
    // The procedure's batch can run several statements: the same rules as the Python worker.
    if (value.changeTracking === 'change_procedure') {
      if (!objectName.test(value.sourceTableName)) {
        context.addIssue({
          code: 'custom',
          path: ['sourceTableName'],
          message: 'Use a plain table name, such as dbo.ORDERS, with a change procedure',
        });
      }
      if (value.keyColumns?.trim() && !columnNames.test(value.keyColumns.trim())) {
        context.addIssue({
          code: 'custom',
          path: ['keyColumns'],
          message: 'Use comma-separated column names with a change procedure',
        });
      }
    }
    if (value.autoCdcFromSnapshot && value.ingestionType !== 'full') {
      context.addIssue({
        code: 'custom',
        path: ['autoCdcFromSnapshot'],
        message: 'Snapshot CDC requires full ingestion',
      });
    }
    if (value.autoCdcFromSnapshot && !value.stagingTableFqn?.trim()) {
      context.addIssue({ code: 'custom', path: ['stagingTableFqn'], message: 'Snapshot CDC requires a staging table' });
    }
    if (value.autoCdcFromSnapshot && !value.keyColumns?.trim()) {
      context.addIssue({ code: 'custom', path: ['keyColumns'], message: 'Snapshot CDC requires key columns' });
    }
    if (value.ingestionType !== 'incremental') return;
    if (!value.keyColumns?.trim()) {
      context.addIssue({
        code: 'custom',
        path: ['keyColumns'],
        message: 'Incremental ingestion requires a key column',
      });
    }
    if (!usesChangeSource(value.changeTracking) && !value.watermarkColumn?.trim()) {
      context.addIssue({
        code: 'custom',
        path: ['watermarkColumn'],
        message: 'Incremental ingestion requires a watermark column, unless it uses a change table or change procedure',
      });
    }
  });

/** A change_tracking_profiles row: how a source system records changes. Same rules as validate_profile in Python. */
export const profileSchema = locationSchema
  .extend({
    originalProfileName: z.string().optional(),
    profileName: z.string().trim().min(1).max(200),
    description: optionalText(z.string().trim()),
    method: z.enum(profileMethods),
    changeTableName: optionalText(z.string().trim()),
    sequenceColumn: optionalText(z.string().trim()),
    deleteFlagColumn: optionalText(z.string().trim()),
    changeTimeColumn: z.string().trim().regex(columnName, 'Use a column name'),
    // The SQL template keeps its line breaks; only the ends are trimmed.
    procedureCall: optionalText(z.string().trim()),
    resultsTable: optionalText(z.string().trim()),
  })
  .superRefine((value, context) => {
    const issue = (path: string, message: string) => context.addIssue({ code: 'custom', path: [path], message });
    const checkNameTemplate = (path: 'changeTableName' | 'resultsTable', template: string) => {
      const unknown = unknownPlaceholders(template, namePlaceholders);
      if (unknown.length) issue(path, `Unknown placeholder {${unknown[0]}}; use {schema}, {table} or {table_lower}`);
      else if (!objectName.test(template.replace(/\{[A-Za-z_]+\}/g, 'x'))) issue(path, 'Use a table name template');
    };
    if (value.method === 'change_table') {
      if (!value.changeTableName) issue('changeTableName', 'Required for change tables');
      else {
        checkNameTemplate('changeTableName', value.changeTableName);
        const used = placeholdersOf(value.changeTableName);
        if (!used.has('table') && !used.has('table_lower')) {
          issue('changeTableName', 'Use {table} or {table_lower}, so each source table has its own change table');
        }
      }
      for (const path of ['sequenceColumn', 'deleteFlagColumn'] as const) {
        if (!value[path] || !columnName.test(value[path])) issue(path, 'Use a column name');
      }
      return;
    }
    const call = value.procedureCall ?? '';
    if (!call) {
      issue('procedureCall', 'Required for a change procedure');
      return;
    }
    const unknown = unknownPlaceholders(call, procedurePlaceholders);
    if (unknown.length) issue('procedureCall', `Unknown placeholder {${unknown[0]}}`);
    const used = placeholdersOf(call);
    for (const name of ['start', 'end']) {
      if (!used.has(name)) issue('procedureCall', `The call must contain {${name}}`);
    }
    if (!used.has('temp_table') && !value.resultsTable) {
      issue('procedureCall', 'Use {temp_table}, or set the results table the procedure writes to');
    }
    if (value.resultsTable) checkNameTemplate('resultsTable', value.resultsTable);
  });
export const sourceDatabaseType = z.enum(['postgresql', 'mysql', 'sqlserver', 'oracle']);
export const sourceDiscoverySchema = z
  .object({
    connectionName: z.string().trim().optional().default(''),
    jdbcUrl: z.string().trim().optional().default(''),
    jdbcUser: z.string().trim().optional().default(''),
    jdbcSecretScope: z.string().trim().optional().default(''),
    jdbcSecretKey: z.string().trim().optional().default(''),
    ucSecretName: z.string().trim().optional().default(''),
    database: z.string().trim().optional().default(''),
    databaseType: sourceDatabaseType,
    tableFilter: z.string().trim().max(200).optional().default(''),
  })
  .superRefine((value, context) => {
    if (value.connectionName) return;
    for (const field of ['database', 'jdbcUrl', 'jdbcUser'] as const) {
      if (!value[field]) {
        context.addIssue({ code: 'custom', path: [field], message: 'Required for direct JDBC discovery' });
      }
    }
    if (!value.ucSecretName) {
      for (const field of ['jdbcSecretScope', 'jdbcSecretKey'] as const) {
        if (!value[field]) {
          context.addIssue({
            code: 'custom',
            path: [field],
            message: 'Required for direct JDBC discovery (or provide ucSecretName)',
          });
        }
      }
    }
    if (/(?:password|pwd)\s*=/i.test(value.jdbcUrl)) {
      context.addIssue({
        code: 'custom',
        path: ['jdbcUrl'],
        message: 'Do not put passwords in the JDBC URL; use a Databricks secret scope and key',
      });
    }
  });
export const sourceColumnsSchema = sourceDiscoverySchema.extend({
  sourceSchema: z.string().trim().min(1),
  table: z.string().trim().min(1),
});
export const sourceColumnsBatchSchema = sourceDiscoverySchema.extend({
  tables: z
    .array(z.object({ sourceSchema: z.string().trim().min(1), table: z.string().trim().min(1) }))
    .min(1)
    .max(10),
});
export const keySchema = locationSchema.extend({
  ingestionGroup: z.string().min(1),
  sourceTableName: z.string().min(1),
});
export const profileKeySchema = locationSchema.extend({ profileName: z.string().trim().min(1) });
export const watermarkSchema = keySchema.extend({ lastWatermark: z.string().nullable(), status: z.string().min(1) });
export const cronExpression = z
  .string()
  .trim()
  .refine((value) => value.split(/\s+/).length >= 6, 'Use a Quartz cron expression with at least 6 fields');
export const scheduleSchema = z.object({
  enabled: z.boolean(),
  quartzCronExpression: cronExpression,
  timezoneId: z.string().trim().min(1),
  pauseStatus: z.enum(['PAUSED', 'UNPAUSED']),
});
export const computeSchema = z
  .object({
    mode: z.enum(['SERVERLESS', 'JOB_CLUSTER']),
    performanceTarget: z.enum(['STANDARD', 'PERFORMANCE_OPTIMIZED']),
    sparkVersion: z.string().trim(),
    driverNodeTypeId: z.string().trim(),
    workerNodeTypeId: z.string().trim(),
    minWorkers: z.number().int().min(0),
    maxWorkers: z.number().int().min(1),
  })
  .superRefine((value, context) => {
    if (value.mode !== 'JOB_CLUSTER') return;
    for (const [field, fieldValue] of [
      ['sparkVersion', value.sparkVersion],
      ['driverNodeTypeId', value.driverNodeTypeId],
      ['workerNodeTypeId', value.workerNodeTypeId],
    ] as const) {
      if (!fieldValue) context.addIssue({ code: 'custom', path: [field], message: 'Required for a job cluster' });
    }
    if (value.maxWorkers < value.minWorkers) {
      context.addIssue({
        code: 'custom',
        path: ['maxWorkers'],
        message: 'Maximum workers must be at least the minimum',
      });
    }
  });
export const pipelineComputeSchema = z
  .object({
    mode: z.enum(['SERVERLESS', 'CLASSIC']),
    nodeTypeId: z.string().trim().optional().default(''),
    driverNodeTypeId: z.string().trim().optional().default(''),
    minWorkers: z.number().int().min(0).optional().default(1),
    maxWorkers: z.number().int().min(1).optional().default(4),
    photon: z.boolean().optional().default(false),
  })
  .superRefine((value, context) => {
    if (value.mode !== 'CLASSIC') return;
    for (const [field, fieldValue] of [
      ['nodeTypeId', value.nodeTypeId],
      ['driverNodeTypeId', value.driverNodeTypeId],
    ] as const) {
      if (!fieldValue)
        context.addIssue({ code: 'custom', path: [field], message: 'Required for classic pipeline compute' });
    }
    if (value.maxWorkers < value.minWorkers) {
      context.addIssue({
        code: 'custom',
        path: ['maxWorkers'],
        message: 'Maximum workers must be at least the minimum',
      });
    }
  });
const runAsPrincipalIssue = (mode: string, principal: string) => {
  if (mode === 'USER' && !/^[^\s@]+@[^\s@]+$/.test(principal)) return 'Enter the user email';
  if (
    mode === 'SERVICE_PRINCIPAL' &&
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(principal)
  ) {
    return 'Enter the service principal application ID';
  }
  return null;
};
// DEFAULT leaves run_as unset so the resource runs as its owner (the app service principal that creates it).
const runAsTargetSchema = <T extends readonly [string, ...string[]]>(modes: T) =>
  z
    .object({ mode: z.enum(modes), principal: z.string().trim().optional().default('') })
    .superRefine((value, context) => {
      const issue = runAsPrincipalIssue(value.mode, value.principal);
      if (issue) context.addIssue({ code: 'custom', path: ['principal'], message: issue });
    });
export const runAsSchema = z.object({
  job: runAsTargetSchema(['DEFAULT', 'USER', 'SERVICE_PRINCIPAL'] as const).prefault({ mode: 'DEFAULT' }),
  pipeline: runAsTargetSchema(['SAME_AS_JOB', 'DEFAULT', 'USER', 'SERVICE_PRINCIPAL'] as const).prefault({
    mode: 'SAME_AS_JOB',
  }),
});
export type RunAsTarget = { mode: string; principal: string };
export const toRunAs = (target: RunAsTarget) =>
  target.mode === 'USER'
    ? { user_name: target.principal }
    : target.mode === 'SERVICE_PRINCIPAL'
      ? { service_principal_name: target.principal }
      : undefined;
// Where the job's notebooks come from and where its tasks install SyncHole from (see jobSource.ts).
const jobSourceFields = { notebooks: notebooksSchema, library: librarySchema.prefault({ source: 'GIT' }) };
export const jobPayloadSchema = z.preprocess(
  withLegacySource,
  locationSchema
    .extend({
      ingestionGroup: z.string().min(1),
      ...jobSourceFields,
      foreachConcurrency: z.number().int().min(1).max(100),
      cdcPipelineId: z.string().trim().nullable().optional(),
      schedule: scheduleSchema,
      compute: computeSchema,
      pipelineCompute: pipelineComputeSchema.prefault({ mode: 'SERVERLESS' }),
      runAs: runAsSchema.prefault({}),
    })
    .superRefine(checkJobSource)
);
// A detailed check's range start: a number, a date, a timestamp or days back (-30d); empty checks every row.
const rangeBound = z
  .string()
  .trim()
  .regex(
    /^$|^-?\d+(\.\d+)?$|^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$|^-\d{1,5}d$/,
    'Use a number, a date (2026-09-01), a timestamp (2026-09-01 12:00:00) or days back (-30d)'
  );
export const rowCheckJobPayloadSchema = z.preprocess(
  withLegacySource,
  locationSchema
    .extend({
      ...jobSourceFields,
      maxParallelTables: z.number().int().min(1).max(32),
      schedule: scheduleSchema,
      // The same compute and identity options as an ingestion job: not every workspace has serverless jobs.
      compute: computeSchema.prefault({
        mode: 'SERVERLESS',
        performanceTarget: 'PERFORMANCE_OPTIMIZED',
        sparkVersion: '',
        driverNodeTypeId: '',
        workerNodeTypeId: '',
        minWorkers: 1,
        maxWorkers: 4,
      }),
      runAs: runAsSchema.shape.job,
      // Which tables the job checks by default: one ingestion group, or every group ('') with the location's job.
      ingestionGroup: z.string().trim().optional().default(''),
      checkMode: z.enum(['basic', 'detailed']).optional().default('basic'),
      // A detailed check's range start; ignored for a basic check.
      rangeFrom: rangeBound.optional().default(''),
    })
    .superRefine(checkJobSource)
);
export const rowCheckRunPayloadSchema = locationSchema.extend({
  checkMode: z.enum(['basic', 'detailed']),
  // Empty means every group or table; a detailed check usually names one table.
  ingestionGroup: z.string().trim().optional().default(''),
  sourceTableName: z.string().trim().optional().default(''),
  // Optional range of rows to check: rangeFrom <= column < rangeTo. The job validates the values against the
  // column type; an empty column means each table's chunk column.
  rangeColumn: z.string().trim().max(128).optional().default(''),
  rangeFrom: z.string().trim().max(64).optional().default(''),
  rangeTo: z.string().trim().max(64).optional().default(''),
});
// GET /api/jobs: the groups of one page of the Jobs page (at most 100, the largest page size).
export const jobsListQuerySchema = locationSchema.extend({
  group: z.array(z.string().min(1)).max(100),
});
export const jobSchedulePayloadSchema = scheduleSchema.extend({ jobId: z.number().int().positive() });

// Entries are kept loose here and validated one by one against configSchema / jobPayloadSchema, with the
// import's target location filled in, so a file exported from one catalog.schema can be loaded into another.
export const transferFormat = 'synchole-config';
export const transferVersion = 1;
export const transferFileSchema = z.object({
  format: z.literal(transferFormat, { error: 'Not a SyncHole configuration export' }),
  version: z.literal(transferVersion, { error: `Unsupported export version (expected ${transferVersion})` }),
  profiles: z.array(z.record(z.string(), z.unknown())).max(500).optional().default([]),
  configs: z.array(z.record(z.string(), z.unknown())).max(5000).optional().default([]),
  jobs: z.array(z.record(z.string(), z.unknown())).max(500).optional().default([]),
  // Row check jobs: the location's (ingestionGroup '') and each group's, each with its check mode.
  rowCheckJobs: z.array(z.record(z.string(), z.unknown())).max(1000).optional().default([]),
});
export const importPayloadSchema = locationSchema.extend({
  file: transferFileSchema,
  // Delete rows of the imported groups that are not in the file, so each group matches the file exactly.
  replaceGroups: z.boolean().optional().default(false),
  // Create or update the Databricks job (and CDC pipeline) of every group with job settings in the file.
  deployJobs: z.boolean().optional().default(false),
  // Validate and summarize without writing anything.
  dryRun: z.boolean().optional().default(false),
});

/**
 * The change-tracking columns as stored: a profile only for change tables and change procedures, and the
 * procedure settings only with a change procedure. Shared by the single-row save and the import.
 */
export const changeTrackingColumns = (config: ConfigPayload) => {
  const changeSource = usesChangeSource(config.changeTracking);
  const procedure = config.changeTracking === 'change_procedure';
  return {
    change_tracking: config.ingestionType === 'incremental' ? config.changeTracking : 'watermark',
    change_profile: changeSource ? (config.changeProfile ?? null) : null,
    change_columns: procedure ? (config.changeColumns ?? null) : null,
    skip_change_time: procedure && config.skipChangeTime,
    watermark_column: config.ingestionType === 'incremental' && !changeSource ? (config.watermarkColumn ?? null) : null,
  };
};

export type SourceDiscovery = z.infer<typeof sourceDiscoverySchema>;
export type SourceDatabaseType = z.infer<typeof sourceDatabaseType>;
export type PipelineCompute = z.infer<typeof pipelineComputeSchema>;
export type JobCompute = z.infer<typeof computeSchema>;
export type Schedule = z.infer<typeof scheduleSchema>;
export type ConfigPayload = z.infer<typeof configSchema>;
export type ProfilePayload = z.infer<typeof profileSchema>;
export type JobPayload = z.infer<typeof jobPayloadSchema>;
export type RowCheckJobPayload = z.infer<typeof rowCheckJobPayloadSchema>;
/** Row check job settings as exported: the payload without the location it was deployed to. */
export type RowCheckJobSettings = Omit<RowCheckJobPayload, 'catalog' | 'schema'>;
export type RowCheckRunPayload = z.infer<typeof rowCheckRunPayloadSchema>;
/** Job settings as stored and exported: the job payload without the location it was deployed to. */
export type JobSettings = Omit<JobPayload, 'catalog' | 'schema'>;
