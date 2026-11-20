import { readFileSync } from 'node:fs';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import { importPayloadSchema } from './schemas.js';

type Parameter = { name: string; value?: string };
const execute = vi.fn<(statement: string, parameters?: Parameter[]) => Promise<unknown>>();
const saveJob = vi.fn();
vi.mock('./sql.js', () => ({
  execute,
  ensureMigrations: vi.fn(),
  parameter: (name: string, value: string | null | undefined, type = 'STRING') => ({ name, type, value }),
  tableName: (catalog: string, schema: string, table: string) => `\`${catalog}\`.\`${schema}\`.\`${table}\``,
  responseRows: () => [{ num_affected_rows: '2' }],
  allResponseRows: vi.fn(),
}));
vi.mock('./ingestionJobs.js', () => ({ saveJob, readJobSettings: vi.fn() }));
const saveRowCheckJob = vi.fn();
vi.mock('./rowChecks.js', () => ({ saveRowCheckJob, readRowCheckJobSettings: vi.fn() }));

const { validateImport, importSummary, importConfiguration } = await import('./transfer.js');

const config = (overrides: Record<string, unknown> = {}) => ({
  ingestionGroup: 'sales',
  sourceTableName: 'public.orders',
  targetTableFqn: 'main.bronze.orders',
  ingestionType: 'incremental',
  keyColumns: 'id',
  watermarkColumn: 'updated_at',
  changeTracking: 'watermark',
  autoCdcFromSnapshot: false,
  connectionName: null,
  connectionDatabase: 'ignored_without_connection',
  watermarkThresholdMinutes: 5,
  fetchSize: 10000,
  numPartitions: 8,
  enabled: true,
  ...overrides,
});
const job = (overrides: Record<string, unknown> = {}) => ({
  ingestionGroup: 'sales',
  gitUrl: 'https://github.com/databricks-industry-solutions/synchole',
  gitBranch: 'main',
  foreachConcurrency: 4,
  schedule: { enabled: false, quartzCronExpression: '0 0 8 * * ?', timezoneId: 'UTC', pauseStatus: 'PAUSED' },
  compute: {
    mode: 'SERVERLESS',
    performanceTarget: 'STANDARD',
    sparkVersion: '',
    driverNodeTypeId: '',
    workerNodeTypeId: '',
    minWorkers: 1,
    maxWorkers: 4,
  },
  ...overrides,
});
const location = { catalog: 'dev', schema: 'synchole' };
// A made-up change procedure and change tables: every source system names them differently.
const procedureProfile = (overrides: Record<string, unknown> = {}) => ({
  profileName: 'p',
  method: 'change_procedure',
  changeTimeColumn: 'CHANGED_AT',
  procedureCall: "EXEC audit.list_changed_rows @from = {start}, @to = {end}, @into = N'{temp_table}'",
  ...overrides,
});
const tableProfile = (overrides: Record<string, unknown> = {}) => ({
  profileName: 't',
  method: 'change_table',
  changeTableName: 'audit.{table_lower}_changes',
  sequenceColumn: 'CHANGE_SEQ',
  deleteFlagColumn: 'IS_DELETED',
  changeTimeColumn: 'CHANGED_AT',
  ...overrides,
});

describe('validateImport', () => {
  it('retargets entries at the import location and applies schema defaults', () => {
    const { configs, jobs } = validateImport({
      ...location,
      file: { configs: [{ ...config(), catalog: 'prod', schema: 'other' }], jobs: [job()] },
    });
    expect(configs[0]).toMatchObject({ catalog: 'dev', schema: 'synchole', sourceTableName: 'public.orders' });
    expect(jobs[0]).toMatchObject({
      catalog: 'dev',
      pipelineCompute: { mode: 'SERVERLESS' },
      runAs: { job: { mode: 'DEFAULT' }, pipeline: { mode: 'SAME_AS_JOB' } },
    });
  });

  it('defaults change tracking to the watermark column', () => {
    const { configs } = validateImport({
      ...location,
      file: { configs: [config({ changeTracking: undefined })], jobs: [] },
    });
    expect(configs[0]).toMatchObject({ changeTracking: 'watermark', skipChangeTime: false });
  });

  it('rejects change-tracking settings that are invalid or name a profile the file lacks', () => {
    const proc = { watermarkColumn: null, changeTracking: 'change_procedure', changeProfile: 'p' };
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          profiles: [procedureProfile()],
          configs: [
            config({ changeTracking: 'cdc' }),
            config({ ...proc, sourceTableName: 'a', changeProfile: null }),
            config({ ...proc, sourceTableName: 'b', changeColumns: "A'--" }),
            config({ ...proc, sourceTableName: 'dbo.T; DROP TABLE x' }),
            config({ ...proc, sourceTableName: 'dbo.e', keyColumns: 'ID) AND 1=1; EXEC x; SELECT (ID' }),
            config({ ...proc, sourceTableName: 'dbo.f', changeProfile: 'missing' }),
            config({ ...proc, sourceTableName: 'dbo.g', changeTracking: 'change_table' }),
          ],
          jobs: [],
        },
      });
    } catch (caught) {
      error = caught;
    }
    const issues = (error as z.ZodError).issues;
    expect(issues.map((issue) => issue.path.join('.'))).toEqual([
      'file.configs.0.changeTracking',
      'file.configs.1.changeProfile',
      'file.configs.2.changeColumns',
      'file.configs.3.sourceTableName',
      'file.configs.4.keyColumns',
      'file.configs.5.changeProfile',
      'file.configs.6.changeProfile',
    ]);
    expect(issues[5].message).toBe("Profile 'missing' is not in the file");
    expect(issues[6].message).toBe("Profile 'p' is for change procedure");
  });

  it('rejects profiles that could not run', () => {
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          profiles: [
            tableProfile({ changeTableName: 'audit.all_changes' }),
            tableProfile({ profileName: 't2', sequenceColumn: 'A; DROP TABLE x' }),
            procedureProfile({ procedureCall: 'EXEC p {start}, {password}' }),
            procedureProfile({ profileName: 'p2', procedureCall: 'EXEC p {start}, {end}' }),
            procedureProfile({ profileName: 'p3', changeTimeColumn: 'x y' }),
            tableProfile({ profileName: 'ok' }),
            tableProfile({ profileName: 'ok' }),
          ],
          configs: [],
          jobs: [],
        },
      });
    } catch (caught) {
      error = caught;
    }
    const issues = (error as z.ZodError).issues;
    expect(issues.map((issue) => issue.path.join('.'))).toEqual([
      'file.profiles.0.changeTableName',
      'file.profiles.1.sequenceColumn',
      // An unknown placeholder, no {end}, and neither {temp_table} nor a results table.
      'file.profiles.2.procedureCall',
      'file.profiles.2.procedureCall',
      'file.profiles.2.procedureCall',
      'file.profiles.3.procedureCall',
      'file.profiles.4.changeTimeColumn',
      'file.profiles.6',
    ]);
    expect(issues[2].message).toBe('Unknown placeholder {password}');
  });

  it('reports every invalid entry and duplicate with its position in the file', () => {
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          configs: [config(), config({ watermarkColumn: null }), config()],
          jobs: [job({ gitUrl: 'https://git.example.org/x/y' })],
        },
      });
    } catch (caught) {
      error = caught;
    }
    expect(error).toBeInstanceOf(z.ZodError);
    const paths = (error as z.ZodError).issues.map((issue) => issue.path.join('.'));
    expect(paths).toEqual(
      expect.arrayContaining(['file.configs.1.watermarkColumn', 'file.jobs.0.notebooks.gitProvider', 'file.configs.2'])
    );
  });
});

describe('examples/synchole-config.json', () => {
  it('is a valid import file, as the Import dialog and the CLI read it', () => {
    const file: unknown = JSON.parse(
      readFileSync(new URL('../../../examples/synchole-config.json', import.meta.url), 'utf8')
    );
    // The same steps as POST /api/import.
    const body = importPayloadSchema.parse({ ...location, file });
    const { profiles, configs, jobs, rowCheckJobs } = validateImport(body);
    expect(profiles.map((profile) => [profile.profileName, profile.method])).toEqual([
      ['source-change-tables', 'change_table'],
      ['source-change-procedure', 'change_procedure'],
    ]);
    expect(importSummary(configs, jobs, rowCheckJobs)).toEqual([
      { ingestionGroup: 'finance', configCount: 2, hasJobSettings: true, rowChecks: [] },
      { ingestionGroup: 'source_change_procedure', configCount: 3, hasJobSettings: true, rowChecks: [] },
      { ingestionGroup: 'source_change_tables', configCount: 3, hasJobSettings: true, rowChecks: ['detailed'] },
      { ingestionGroup: 'source_reference', configCount: 2, hasJobSettings: true, rowChecks: ['detailed'] },
    ]);
    // Plus the basic check of the whole location.
    expect(rowCheckJobs).toHaveLength(3);
  });
});

describe('importSummary', () => {
  it('counts source tables per group and flags groups with job settings', () => {
    const { configs, jobs } = validateImport({
      ...location,
      file: {
        configs: [config(), config({ sourceTableName: 'public.items' }), config({ ingestionGroup: 'hr' })],
        jobs: [job()],
      },
    });
    expect(importSummary(configs, jobs)).toEqual([
      { ingestionGroup: 'hr', configCount: 1, hasJobSettings: false, rowChecks: [] },
      { ingestionGroup: 'sales', configCount: 2, hasJobSettings: true, rowChecks: [] },
    ]);
  });
});

describe('row check jobs in a file', () => {
  const rowCheckJob = (overrides: Record<string, unknown> = {}) => ({
    gitUrl: 'https://github.com/databricks-industry-solutions/synchole',
    gitBranch: 'main',
    maxParallelTables: 4,
    schedule: { enabled: true, quartzCronExpression: '0 0 6 * * ?', timezoneId: 'UTC', pauseStatus: 'UNPAUSED' },
    ...overrides,
  });

  it("lists each group's checks; the location's checks are not a group", () => {
    const { configs, jobs, rowCheckJobs } = validateImport({
      ...location,
      file: {
        configs: [config()],
        jobs: [job()],
        rowCheckJobs: [
          rowCheckJob(),
          rowCheckJob({ ingestionGroup: 'sales' }),
          rowCheckJob({ ingestionGroup: 'sales', checkMode: 'detailed', rangeFrom: '-30d' }),
        ],
      },
    });
    expect(rowCheckJobs[0]).toMatchObject({ catalog: 'dev', ingestionGroup: '', checkMode: 'basic' });
    expect(importSummary(configs, jobs, rowCheckJobs)).toEqual([
      { ingestionGroup: 'sales', configCount: 1, hasJobSettings: true, rowChecks: ['basic', 'detailed'] },
    ]);
  });

  it('rejects a range that the check could not parse, and two jobs for one scope and mode', () => {
    let error: unknown;
    try {
      validateImport({
        ...location,
        file: {
          configs: [config()],
          jobs: [],
          rowCheckJobs: [
            rowCheckJob({ checkMode: 'detailed', rangeFrom: 'last month' }),
            rowCheckJob({ ingestionGroup: 'sales' }),
            rowCheckJob({ ingestionGroup: 'sales' }),
          ],
        },
      });
    } catch (caught) {
      error = caught;
    }
    const issues = (error as z.ZodError).issues;
    expect(issues.map((issue) => issue.path.join('.'))).toEqual([
      'file.rowCheckJobs.0.rangeFrom',
      'file.rowCheckJobs.2',
    ]);
    expect(issues[0].message).toMatch(/days back/);
  });

  it('deploys them with the jobs, and keeps going when one fails', async () => {
    const entries = validateImport({
      ...location,
      file: {
        configs: [config()],
        jobs: [],
        rowCheckJobs: [rowCheckJob(), rowCheckJob({ ingestionGroup: 'sales', checkMode: 'detailed' })],
      },
    });
    saveRowCheckJob
      .mockRejectedValueOnce(new Error('PERMISSION_DENIED'))
      .mockResolvedValueOnce({ jobId: 12, action: 'created', name: '[sales] Row Check (detailed)' });
    const result = await importConfiguration('dev', 'synchole', entries, { replaceGroups: false, deployJobs: true });
    expect(result.rowCheckJobs).toEqual([
      { ingestionGroup: '', checkMode: 'basic', error: 'PERMISSION_DENIED' },
      {
        ingestionGroup: 'sales',
        checkMode: 'detailed',
        jobId: 12,
        action: 'created',
        name: '[sales] Row Check (detailed)',
      },
    ]);
  });
});

describe('importConfiguration', () => {
  beforeEach(() => {
    execute.mockReset();
    saveJob.mockReset();
  });

  it('upserts normalized rows, then removes unlisted rows, then deploys jobs', async () => {
    const entries = validateImport({
      ...location,
      file: { configs: [config(), config({ sourceTableName: 'public.items', ingestionType: 'full' })], jobs: [job()] },
    });
    saveJob.mockResolvedValue({ jobId: 7, pipelineId: 'p', action: 'created' });

    const result = await importConfiguration('dev', 'synchole', entries, { replaceGroups: true, deployJobs: true });

    const [[merge, mergeParameters], [remove, removeParameters]] = execute.mock.calls;
    expect(merge).toContain('MERGE INTO `dev`.`synchole`.`jdbc_ingestion_config`');
    const rows = JSON.parse(mergeParameters![0].value!) as Record<string, unknown>[];
    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ source_table_name: 'public.orders', watermark_column: 'updated_at' });
    // Full loads never keep a watermark column, and the database override needs a UC connection.
    expect(rows[1]).toMatchObject({ watermark_column: null, connection_database: null });
    expect(remove).toContain('DELETE FROM');
    expect(JSON.parse(removeParameters![1].value!)).toEqual([
      ['sales', 'public.orders'],
      ['sales', 'public.items'],
    ]);
    expect(saveJob).toHaveBeenCalledWith(expect.objectContaining({ ingestionGroup: 'sales', catalog: 'dev' }));
    expect(result).toEqual({
      profileCount: 0,
      configCount: 2,
      removedCount: 2,
      jobs: [{ ingestionGroup: 'sales', jobId: 7, pipelineId: 'p', action: 'created' }],
      rowCheckJobs: [],
    });
  });

  it('writes the profiles first, and stores change-tracking settings only where they apply', async () => {
    const entries = validateImport({
      ...location,
      file: {
        profiles: [procedureProfile({ resultsTable: 'audit.unused' }), tableProfile({ procedureCall: 'unused' })],
        configs: [
          config({
            watermarkColumn: null,
            changeTracking: 'change_procedure',
            changeProfile: 'p',
            changeColumns: 'A, B',
            skipChangeTime: true,
          }),
          config({
            sourceTableName: 'public.changes',
            watermarkColumn: 'ignored',
            changeTracking: 'change_table',
            changeProfile: 't',
            changeColumns: 'A',
            skipChangeTime: true,
          }),
          config({ sourceTableName: 'public.plain', changeProfile: 'p', changeColumns: 'A' }),
        ],
        jobs: [],
      },
    });

    const result = await importConfiguration('dev', 'synchole', entries, { replaceGroups: false, deployJobs: false });

    const [[profileMerge, profileParameters], [configMerge, configParameters]] = execute.mock.calls;
    expect(profileMerge).toContain('MERGE INTO `dev`.`synchole`.`change_tracking_profiles`');
    const profiles = JSON.parse(profileParameters![0].value!) as Record<string, unknown>[];
    // Only the columns of each profile's method are kept.
    expect(profiles[0]).toMatchObject({ profile_name: 'p', results_table: 'audit.unused', change_table_name: null });
    expect(profiles[1]).toMatchObject({ profile_name: 't', procedure_call: null, sequence_column: 'CHANGE_SEQ' });
    expect(configMerge).toContain('MERGE INTO `dev`.`synchole`.`jdbc_ingestion_config`');
    const rows = JSON.parse(configParameters![0].value!) as Record<string, unknown>[];
    expect(rows[0]).toMatchObject({
      change_tracking: 'change_procedure',
      change_profile: 'p',
      change_columns: 'A, B',
      skip_change_time: true,
      watermark_column: null,
    });
    // Procedure settings only with a change procedure; no watermark column where changes come from a profile.
    expect(rows[1]).toMatchObject({
      change_tracking: 'change_table',
      change_profile: 't',
      change_columns: null,
      skip_change_time: false,
      watermark_column: null,
    });
    expect(rows[2]).toMatchObject({
      change_tracking: 'watermark',
      change_profile: null,
      change_columns: null,
      watermark_column: 'updated_at',
    });
    expect(result.profileCount).toBe(2);
  });

  it('deploys a job with the volume wheel it was exported with', async () => {
    const wheel = '/Volumes/main/synchole/libs/synchole-0.1.0-py3-none-any.whl';
    const entries = validateImport({
      ...location,
      file: { configs: [config()], jobs: [job({ libraryPath: wheel }), job({ ingestionGroup: 'hr' })] },
    });
    saveJob.mockResolvedValue({ jobId: 7, action: 'created' });

    await importConfiguration('dev', 'synchole', entries, { replaceGroups: false, deployJobs: true });

    expect(saveJob).toHaveBeenCalledWith(
      expect.objectContaining({ ingestionGroup: 'sales', library: { source: 'WHEEL', path: wheel } })
    );
    // A file written before the setting existed installs from the GitHub branch.
    expect(saveJob).toHaveBeenCalledWith(expect.objectContaining({ ingestionGroup: 'hr', library: { source: 'GIT' } }));
  });

  it('keeps going when a job fails and leaves other rows alone without replaceGroups', async () => {
    const entries = validateImport({
      ...location,
      file: { configs: [config()], jobs: [job(), job({ ingestionGroup: 'hr' })] },
    });
    saveJob
      .mockRejectedValueOnce(new Error('PERMISSION_DENIED'))
      .mockResolvedValueOnce({ jobId: 9, action: 'updated' });

    const result = await importConfiguration('dev', 'synchole', entries, { replaceGroups: false, deployJobs: true });

    expect(execute).toHaveBeenCalledTimes(1);
    expect(result.jobs).toEqual([
      { ingestionGroup: 'sales', error: 'PERMISSION_DENIED' },
      { ingestionGroup: 'hr', jobId: 9, action: 'updated' },
    ]);
  });
});
