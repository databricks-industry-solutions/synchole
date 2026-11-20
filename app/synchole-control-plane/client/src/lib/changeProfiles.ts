import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { useAnalyticsQuery } from '@databricks/appkit-ui/react';
import type { Location } from '../types';
import { fqn } from '../utils';
import { useReportedFailure } from './logging';

export const methodLabels: Record<string, string> = {
  watermark: 'Watermark column',
  change_table: 'Change table',
  change_procedure: 'Change procedure',
};

/** The location's change-tracking profiles, for the profile list and the pickers of the config editors. */
export function useChangeProfiles(location: Location, revision: number) {
  const params = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'change_tracking_profiles')),
      config_table_name: sql.string(fqn(location, 'jdbc_ingestion_config')),
      refresh_token: sql.int(revision),
    }),
    [location, revision]
  );
  const query = useAnalyticsQuery('change_profiles', params);
  useReportedFailure('query.change_profiles', query.error, location);
  return query;
}
