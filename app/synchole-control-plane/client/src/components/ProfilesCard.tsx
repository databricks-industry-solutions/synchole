import { useEffect, useState } from 'react';
import type { FormEvent } from 'react';
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Textarea,
} from '@databricks/appkit-ui/react';
import { Plus, Trash2 } from 'lucide-react';
import { api } from '../lib/api';
import { methodLabels, useChangeProfiles } from '../lib/changeProfiles';
import { reportError } from '../lib/logging';
import type { Location, ProfileRow } from '../types';
import { ErrorState, Field } from './shared';

/** The profile picker of a config editor: only the profiles of the chosen method. */
export function ProfileSelect({
  id,
  name,
  method,
  profiles,
  value,
  onValueChange,
}: {
  id: string;
  name?: string;
  method: string;
  profiles: ProfileRow[];
  value: string;
  onValueChange: (value: string) => void;
}) {
  const choices = profiles.filter((profile) => profile.method === method);
  return (
    <div className="space-y-2">
      <Label htmlFor={id}>Change-tracking profile</Label>
      <Select name={name} value={value} onValueChange={onValueChange}>
        <SelectTrigger id={id} className="w-full">
          <SelectValue placeholder={choices.length ? 'Choose a profile' : 'No profile for this method'} />
        </SelectTrigger>
        <SelectContent>
          {choices.map((profile) => (
            <SelectItem key={profile.profile_name} value={profile.profile_name}>
              {profile.profile_name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <p className="text-xs text-muted-foreground">
        {choices.length
          ? 'How the source system records changes; profiles are managed under Change-tracking profiles.'
          : 'Add a profile under Change-tracking profiles first.'}
      </p>
    </div>
  );
}

/** The location's change-tracking profiles: how each source system records changes, shared by its tables. */
export function ProfilesCard({
  location,
  revision,
  onChanged,
}: {
  location: Location;
  revision: number;
  onChanged: () => void;
}) {
  const { data, loading, error } = useChangeProfiles(location, revision);
  const [editing, setEditing] = useState<ProfileRow | null>(null);
  const [open, setOpen] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const remove = async (profile: ProfileRow) => {
    if (!confirm(`Delete profile ${profile.profile_name}?`)) return;
    try {
      await api<unknown>('/api/profiles', {
        method: 'DELETE',
        body: JSON.stringify({ ...location, profileName: profile.profile_name }),
      });
      onChanged();
    } catch (e) {
      setMessage(reportError('profile.delete', e, { ...location, profileName: profile.profile_name }));
    }
  };
  return (
    <Card className="mt-6">
      <CardHeader className="flex flex-row items-start justify-between gap-4">
        <div className="space-y-1.5">
          <CardTitle className="text-base">Change-tracking profiles</CardTitle>
          <CardDescription>
            How a source system records changes, for tables that use a change table or a change procedure instead of a
            watermark column. The table and column names come from the source system&apos;s documentation.
          </CardDescription>
        </div>
        <Button
          size="sm"
          variant="outline"
          onClick={() => {
            setEditing(null);
            setOpen(true);
          }}
        >
          <Plus className="mr-2 h-4 w-4" /> Add profile
        </Button>
      </CardHeader>
      <CardContent>
        {message && <ErrorState message={message} />}
        {loading ? (
          <Skeleton className="h-24" />
        ) : error ? (
          <ErrorState message={error} />
        ) : !data?.length ? (
          <p className="text-sm text-muted-foreground">No profiles. Tables can still use a watermark column.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Profile</TableHead>
                <TableHead>Method</TableHead>
                <TableHead>Changes read from</TableHead>
                <TableHead>Used by</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((profile) => (
                <TableRow key={profile.profile_name}>
                  <TableCell>
                    <div className="font-medium">{profile.profile_name}</div>
                    {profile.description && <div className="text-xs text-muted-foreground">{profile.description}</div>}
                  </TableCell>
                  <TableCell>
                    <Badge variant="secondary">{methodLabels[profile.method] ?? profile.method}</Badge>
                  </TableCell>
                  <TableCell className="max-w-md truncate font-mono text-xs">
                    {profile.method === 'change_table' ? profile.change_table_name : profile.procedure_call}
                  </TableCell>
                  <TableCell>{Number(profile.used_by)} tables</TableCell>
                  <TableCell className="text-right">
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        setEditing(profile);
                        setOpen(true);
                      }}
                    >
                      Edit
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      aria-label="Delete profile"
                      onClick={() => void remove(profile)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </CardContent>
      <ProfileDialog
        open={open}
        onOpenChange={setOpen}
        profile={editing}
        location={location}
        onSaved={() => {
          setOpen(false);
          onChanged();
        }}
      />
    </Card>
  );
}

function ProfileDialog({
  open,
  onOpenChange,
  profile,
  location,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (value: boolean) => void;
  profile: ProfileRow | null;
  location: Location;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [method, setMethod] = useState(profile?.method ?? 'change_table');
  useEffect(() => setMethod(profile?.method ?? 'change_table'), [profile, open]);
  useEffect(() => setError(null), [open]);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const f = new FormData(e.currentTarget);
    try {
      await api('/api/profiles', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          originalProfileName: profile?.profile_name,
          profileName: f.get('profile_name'),
          description: f.get('description') || null,
          method,
          changeTableName: f.get('change_table_name') || null,
          sequenceColumn: f.get('sequence_column') || null,
          deleteFlagColumn: f.get('delete_flag_column') || null,
          changeTimeColumn: f.get('change_time_column'),
          procedureCall: f.get('procedure_call') || null,
          resultsTable: f.get('results_table') || null,
        }),
      });
      onSaved();
    } catch (x) {
      setError(reportError('profile.save', x, { ...location, method }));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>{profile ? 'Edit change-tracking profile' : 'Add change-tracking profile'}</DialogTitle>
          <DialogDescription>
            Templates use placeholders that SyncHole fills in for each source table and each window.
          </DialogDescription>
        </DialogHeader>
        <form
          key={`${profile?.profile_name ?? 'new'}-${open}`}
          onSubmit={(event) => void submit(event)}
          className="space-y-4"
        >
          {error && <ErrorState message={error} />}
          <div className="grid gap-4 md:grid-cols-2">
            <Field name="profile_name" label="Profile name" value={profile?.profile_name} required />
            <div className="space-y-2">
              <Label htmlFor="profile_method">Method</Label>
              <Select value={method} onValueChange={setMethod}>
                <SelectTrigger id="profile_method" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="change_table">Change table</SelectItem>
                  <SelectItem value="change_procedure">Change procedure</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="md:col-span-2">
              <Field name="description" label="Description" value={profile?.description} />
            </div>
            {method === 'change_table' ? (
              <>
                <div className="md:col-span-2">
                  <Field
                    name="change_table_name"
                    label="Change table"
                    value={profile?.change_table_name}
                    required
                    hint="The change table of each source table, e.g. audit.{table_lower}_changes. Placeholders: {schema}, {table}, {table_lower}."
                  />
                </div>
                <Field
                  name="sequence_column"
                  label="Sequence column"
                  value={profile?.sequence_column}
                  required
                  hint="Ever-increasing; used as the watermark."
                />
                <Field
                  name="delete_flag_column"
                  label="Delete flag column"
                  value={profile?.delete_flag_column}
                  required
                  hint="True when the row was deleted."
                />
                <Field
                  name="change_time_column"
                  label="Change time column"
                  value={profile?.change_time_column}
                  required
                  hint="Orders the changes of a key in the history table."
                />
              </>
            ) : (
              <>
                <div className="space-y-2 md:col-span-2">
                  <Label htmlFor="procedure_call">Procedure call</Label>
                  <Textarea
                    id="procedure_call"
                    name="procedure_call"
                    rows={5}
                    required
                    className="font-mono text-xs"
                    defaultValue={profile?.procedure_call ?? ''}
                  />
                  <p className="text-xs text-muted-foreground">
                    Runs the procedure for one window. {'{start}'} and {'{end}'} are timestamp literals,{' '}
                    {'{temp_table}'} the temp table to fill (SQL Server), {'{multi_key}'} and {'{skip_change_time}'} 1
                    or 0, {'{columns}'} a quoted column list or NULL; {'{schema}'}, {'{table}'} and {'{table_lower}'}{' '}
                    name the source table.
                  </p>
                </div>
                <Field
                  name="change_time_column"
                  label="Change time column"
                  value={profile?.change_time_column}
                  required
                  hint="The change time in the procedure's results."
                />
                <Field
                  name="results_table"
                  label="Results table"
                  value={profile?.results_table}
                  hint="Only for procedures that write a table instead of {temp_table}, e.g. on Postgres."
                />
              </>
            )}
          </div>
          <DialogFooter>
            <Button type="submit" disabled={busy}>
              {busy ? 'Saving…' : 'Save profile'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
