import type { Migration } from './index.js';

/**
 * Creates `change_tracking_profiles`: how each source system records changes (a change table per source
 * table, or a change procedure), shared by the config rows that name a profile in `change_profile`.
 * Same columns as UnityCatalogSetup.create_profiles_table in Python.
 */
export default {
  id: '005_create_change_tracking_profiles',
  description: 'Create change_tracking_profiles table',
  async run(ctx, catalog, schema) {
    await ctx.execute(
      `CREATE TABLE IF NOT EXISTS ${ctx.tableName(catalog, schema, 'change_tracking_profiles')} (
        profile_name STRING NOT NULL,
        description STRING,
        method STRING NOT NULL,
        change_table_name STRING,
        sequence_column STRING,
        delete_flag_column STRING,
        change_time_column STRING,
        procedure_call STRING,
        results_table STRING,
        update_dttm TIMESTAMP
      )`
    );
  },
} satisfies Migration;
