import { expect, test } from '@playwright/test';

// Pages need a metadata location; the app keeps the chosen one in this cookie (see client/src/utils.ts).
test.beforeEach(async ({ context, baseURL }) => {
  const location = {
    catalog: process.env.SYNCHOLE_DEFAULT_CATALOG || 'main',
    schema: process.env.SYNCHOLE_DEFAULT_SCHEMA || 'synchole',
  };
  await context.addCookies([
    { name: 'synchole-location', value: encodeURIComponent(JSON.stringify(location)), url: baseURL },
  ]);
});

test('control plane shell and primary navigation load', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByText('SyncHole Control Plane')).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Pipeline health at a glance' })).toBeVisible();
  const navigation = page.getByRole('navigation');
  await expect(navigation.getByRole('link', { name: 'Configuration' })).toBeVisible();
  await expect(navigation.getByRole('link', { name: 'Watermarks' })).toBeVisible();
  await expect(navigation.getByRole('link', { name: 'Jobs' })).toBeVisible();
  await expect(navigation.getByRole('link', { name: 'Row checks' })).toBeVisible();
});

test('configuration screen exposes lookup and create actions', async ({ page }) => {
  await page.goto('/config');
  await expect(page.getByRole('heading', { name: 'Ingestion configuration' })).toBeVisible();
  await expect(page.getByLabel('Search table names')).toBeVisible();
  await page.getByRole('button', { name: 'Add entry' }).click();
  await expect(page.getByRole('heading', { name: 'Add configuration' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Source mapping' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Incremental settings' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Connection & runtime' })).toBeVisible();
});

test('configuration screen exposes guided JDBC table discovery', async ({ page }) => {
  await page.goto('/config');
  await page.getByRole('button', { name: 'Discover tables' }).click();
  await expect(page.getByRole('heading', { name: 'Discover and configure source tables' })).toBeVisible();
  await expect(page.getByRole('dialog').getByLabel('Ingestion group')).toBeVisible();
  await expect(page.getByLabel('Connection method')).toBeVisible();
  await expect(page.getByLabel('UC connection name')).toBeVisible();
  await expect(page.getByText('Leave empty to use the database defined by the UC connection.')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Load available tables' })).toBeVisible();
  await expect(page.getByLabel('Change tracking')).toBeVisible();
  await page.getByLabel('Connection method').click();
  await page.getByRole('option', { name: 'Direct JDBC URL' }).click();
  await expect(page.getByLabel('Database', { exact: true })).toBeVisible();
  await expect(page.getByLabel('JDBC URL')).toBeVisible();
  await expect(page.getByLabel('Password secret scope')).toBeVisible();
});

test('configuration discovery supports UC secret connection mode', async ({ page }) => {
  await page.goto('/config');
  await page.getByRole('button', { name: 'Discover tables' }).click();
  await page.getByLabel('Connection method').click();
  await page.getByRole('option', { name: 'UC Secret (JDBC)' }).click();
  await expect(page.getByLabel('UC secret name')).toBeVisible();
  await expect(page.getByLabel('JDBC URL')).toBeVisible();
  await expect(page.getByLabel('Database', { exact: true })).toBeVisible();
});

test('jobs screen exposes the guided creation form', async ({ page }) => {
  await page.goto('/jobs');
  await page.getByRole('button', { name: 'Create job' }).click();
  await expect(page.getByRole('heading', { name: 'Create or update a SyncHole job' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Job configuration' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Schedule' })).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Compute' })).toBeVisible();
  await page.getByRole('tab', { name: 'Source & execution' }).click();
  await expect(page.getByLabel('Git repository URL')).toBeVisible();
  await expect(page.getByLabel('Branch')).toBeVisible();
  await expect(
    page.getByText('synchole@git+https://github.com/databricks-industry-solutions/synchole.git@main')
  ).toBeVisible();
  await expect(page.getByLabel('Existing SDP pipeline ID (optional)')).toBeVisible();
});

test('jobs screen exposes run monitoring and workspace navigation', async ({ page }) => {
  await page.goto('/jobs');
  const runs = page.getByText('Last 10 runs').first();
  await expect(runs.or(page.getByText('None of the groups on this page has a job yet.'))).toBeVisible();
  if (await runs.isVisible()) {
    await expect(page.getByRole('link', { name: 'Open in Databricks' }).first()).toBeVisible();
  }
});

test('job dialog shows only the fields of the chosen notebook and library source', async ({ page }) => {
  await page.goto('/jobs');
  await page.getByRole('button', { name: 'Create job' }).click();
  await page.getByRole('tab', { name: 'Source & execution' }).click();
  // A tag pins the run; the reference field follows the chosen type.
  await page.getByLabel('Git reference').click();
  await page.getByRole('option', { name: 'Tag' }).click();
  await expect(page.getByLabel('Tag', { exact: true })).toBeVisible();
  // A self-hosted server asks for its provider.
  await page.getByLabel('Git repository URL').fill('https://git.example.org/data/synchole');
  await expect(page.getByLabel('Git provider')).toBeVisible();
  // Notebooks in the workspace: no Git fields, and no install from Git.
  await page.getByLabel('Notebooks from').click();
  await page.getByRole('option', { name: 'Workspace folder' }).click();
  await expect(page.getByLabel('Workspace folder')).toBeVisible();
  await expect(page.getByLabel('Git repository URL')).toHaveCount(0);
  await expect(page.getByLabel('Wheel path')).toBeVisible();
  await page.getByLabel('Install SyncHole from').click();
  await expect(page.getByRole('option', { name: 'Git repository' })).toHaveCount(0);
  await page.getByRole('option', { name: 'Pip requirement' }).click();
  await expect(page.getByLabel('Pip requirement')).toBeVisible();
});

test('configuration editor offers change tracking through a profile', async ({ page }) => {
  await page.goto('/config');
  await page.getByRole('button', { name: 'Add entry' }).click();
  await page.getByRole('tab', { name: 'Incremental settings' }).click();
  await expect(page.getByLabel('Change-tracking profile')).toHaveCount(0);
  await page.getByLabel('Change tracking').click();
  await page.getByRole('option', { name: 'Change procedure (profile)' }).click();
  await expect(page.getByLabel('Change-tracking profile')).toBeVisible();
  await expect(page.getByLabel('Changed columns')).toBeVisible();
  await expect(page.getByText('Skip change times')).toBeVisible();
  await page.getByLabel('Change tracking').click();
  await page.getByRole('option', { name: 'Change table (profile)' }).click();
  await expect(page.getByLabel('Changed columns')).toHaveCount(0);
});

test('configuration discovery offers change tracking through a profile', async ({ page }) => {
  await page.goto('/config');
  await page.getByRole('button', { name: 'Discover tables' }).click();
  await expect(page.getByLabel('Change-tracking profile')).toHaveCount(0);
  await page.getByLabel('Change tracking').click();
  await expect(page.getByRole('option', { name: 'Change procedure (profile)' })).toBeVisible();
});

test('configuration screen manages change-tracking profiles', async ({ page }) => {
  await page.goto('/config');
  await expect(page.getByText('Change-tracking profiles')).toBeVisible();
  await page.getByRole('button', { name: 'Add profile' }).click();
  await expect(page.getByRole('heading', { name: 'Add change-tracking profile' })).toBeVisible();
  await expect(page.getByLabel('Change table')).toBeVisible();
  await expect(page.getByLabel('Sequence column')).toBeVisible();
  await page.getByLabel('Method').click();
  await page.getByRole('option', { name: 'Change procedure' }).click();
  await expect(page.getByLabel('Procedure call')).toBeVisible();
  await expect(page.getByLabel('Results table')).toBeVisible();
});
