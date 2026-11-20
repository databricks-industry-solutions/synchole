import type { Application } from 'express';
import { z } from 'zod';
import { profileKeySchema, profileSchema, type ConfigPayload, type ProfilePayload } from '../schemas.js';
import { allResponseRows, ensureMigrations, execute, parameter, responseRows, tableName } from '../sql.js';
import { apiRoute, logger } from '../logging.js';

const profilesTable = (catalog: string, schema: string) => tableName(catalog, schema, 'change_tracking_profiles');
const configTable = (catalog: string, schema: string) => tableName(catalog, schema, 'jdbc_ingestion_config');

const invalid = (path: string, message: string) =>
  new z.ZodError([{ code: 'custom', path: [path], message, input: undefined }]);

// Every change_tracking_profiles column carried by an export, with its profile payload field.
export const profileColumns = [
  ['profile_name', 'profileName'],
  ['description', 'description'],
  ['method', 'method'],
  ['change_table_name', 'changeTableName'],
  ['sequence_column', 'sequenceColumn'],
  ['delete_flag_column', 'deleteFlagColumn'],
  ['change_time_column', 'changeTimeColumn'],
  ['procedure_call', 'procedureCall'],
  ['results_table', 'resultsTable'],
] as const satisfies ReadonlyArray<readonly [string, keyof ProfilePayload]>;

/** The profile as stored: only the columns its method uses. Shared by the save and the import. */
export const toProfileRow = (profile: ProfilePayload) => {
  const changeTable = profile.method === 'change_table';
  return {
    profile_name: profile.profileName,
    description: profile.description ?? null,
    method: profile.method,
    change_table_name: changeTable ? (profile.changeTableName ?? null) : null,
    sequence_column: changeTable ? (profile.sequenceColumn ?? null) : null,
    delete_flag_column: changeTable ? (profile.deleteFlagColumn ?? null) : null,
    change_time_column: profile.changeTimeColumn,
    procedure_call: changeTable ? null : (profile.procedureCall ?? null),
    results_table: changeTable ? null : (profile.resultsTable ?? null),
  };
};

const rowStruct = `ARRAY<STRUCT<${profileColumns.map(([column]) => `${column}: STRING`).join(', ')}>>`;

/** Inserts or updates profiles by name. */
export async function upsertProfiles(catalog: string, schema: string, profiles: ProfilePayload[]) {
  if (!profiles.length) return;
  const columns = profileColumns.map(([column]) => column);
  const updates = columns.filter((column) => column !== 'profile_name').map((column) => `${column} = s.${column}`);
  await execute(
    `MERGE INTO ${profilesTable(catalog, schema)} t
     USING (SELECT inline(from_json(:rows, '${rowStruct}'))) s
     ON t.profile_name = s.profile_name
     WHEN MATCHED THEN UPDATE SET ${updates.join(', ')}, update_dttm = current_timestamp()
     WHEN NOT MATCHED THEN INSERT (${columns.join(', ')}, update_dttm)
       VALUES (${columns.map((column) => `s.${column}`).join(', ')}, current_timestamp())`,
    [parameter('rows', JSON.stringify(profiles.map(toProfileRow)))]
  );
}

/** Every profile of a location, as export file entries. */
export async function readProfiles(catalog: string, schema: string) {
  const response = await execute(
    `SELECT ${profileColumns.map(([column]) => column).join(', ')}
     FROM ${profilesTable(catalog, schema)} ORDER BY profile_name`
  );
  return (await allResponseRows(response)).map((row) =>
    Object.fromEntries(profileColumns.map(([column, field]) => [field, row[column] ?? null]))
  );
}

/** The method of each of the named profiles that exist. */
async function profileMethods(catalog: string, schema: string, names: string[]) {
  if (!names.length) return new Map<string, string>();
  const response = await execute(
    `SELECT profile_name, method FROM ${profilesTable(catalog, schema)}
     WHERE array_contains(from_json(:names, 'ARRAY<STRING>'), profile_name)`,
    [parameter('names', JSON.stringify(names))]
  );
  return new Map(responseRows(response).map((row) => [String(row.profile_name), String(row.method)]));
}

/** Rejects a config row whose change profile does not exist or is for the other method. */
export async function checkConfigProfile(config: ConfigPayload) {
  if (config.ingestionType !== 'incremental' || config.changeTracking === 'watermark' || !config.changeProfile) return;
  const method = (await profileMethods(config.catalog, config.schema, [config.changeProfile])).get(
    config.changeProfile
  );
  if (!method) throw invalid('changeProfile', `Profile '${config.changeProfile}' does not exist`);
  if (method !== config.changeTracking) {
    throw invalid('changeProfile', `Profile '${config.changeProfile}' is for ${method.replace('_', ' ')}`);
  }
}

/** How many config rows use the profile, and how many of them with a method other than `method`. */
async function profileUsage(catalog: string, schema: string, profileName: string, method = '') {
  const response = await execute(
    `SELECT count(*) AS used, count_if(change_tracking <> :method) AS other_method
     FROM ${configTable(catalog, schema)}
     WHERE change_profile = :profile_name AND change_tracking IN ('change_table', 'change_procedure')`,
    [parameter('profile_name', profileName), parameter('method', method)]
  );
  const row = responseRows(response)[0] ?? {};
  return { used: Number(row.used ?? 0), otherMethod: Number(row.other_method ?? 0) };
}

/** Registers the change-tracking profile routes; the page reads profiles through change_profiles.sql. */
export function registerProfileRoutes(app: Application) {
  app.post(
    '/api/profiles',
    apiRoute('profile_save', async (req, res) => {
      const body = profileSchema.parse(req.body);
      const { catalog, schema } = body;
      await ensureMigrations(catalog, schema);
      const original = body.originalProfileName?.trim() || body.profileName;
      const renamed = original !== body.profileName;
      if (renamed && (await profileMethods(catalog, schema, [body.profileName])).size) {
        throw invalid('profileName', `A profile named '${body.profileName}' already exists`);
      }
      // The source tables that use the profile name it in change_tracking too; changing the method would break them.
      const usage = await profileUsage(catalog, schema, original, body.method);
      if (usage.otherMethod) {
        throw invalid(
          'method',
          `${usage.otherMethod} source table(s) use this profile with another method; change them first`
        );
      }
      await upsertProfiles(catalog, schema, [body]);
      if (renamed) {
        // The source tables keep their profile under its new name.
        await execute(
          `UPDATE ${configTable(catalog, schema)} SET change_profile = :profile_name, update_dttm = current_timestamp()
           WHERE change_profile = :original`,
          [parameter('profile_name', body.profileName), parameter('original', original)]
        );
        await execute(`DELETE FROM ${profilesTable(catalog, schema)} WHERE profile_name = :original`, [
          parameter('original', original),
        ]);
      }
      logger.info('profile.saved', { catalog, schema, profileName: body.profileName, renamed, method: body.method });
      res.json({ ok: true });
    })
  );

  app.delete(
    '/api/profiles',
    apiRoute('profile_delete', async (req, res) => {
      const body = profileKeySchema.parse(req.body);
      const { catalog, schema, profileName } = body;
      await ensureMigrations(catalog, schema);
      const { used } = await profileUsage(catalog, schema, profileName);
      if (used) throw invalid('profileName', `${used} source table(s) use this profile; change them first`);
      await execute(`DELETE FROM ${profilesTable(catalog, schema)} WHERE profile_name = :profile_name`, [
        parameter('profile_name', profileName),
      ]);
      logger.info('profile.deleted', { catalog, schema, profileName });
      res.json({ ok: true });
    })
  );
}
