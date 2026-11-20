# Job Provisioning

To create jobs without the app, from a configuration file, use `synchole-import --deploy-jobs` (see [Bundle Deployment](Bundle-Deployment)): it builds the same jobs as the app, with compute, schedule, run-as identity and row check jobs. The Control Plane app's **Jobs** page is the recommended way to create jobs interactively: it also configures job and SDP compute, the schedule and run-as identities (see [Jobs created by the app](Control-Plane-App#jobs-created-by-the-app)), and its settings can be exported and imported with the rest of the configuration. From Python:

```python
from synchole.models import JobProvisioningSettings
from synchole.utils import IngestionJobProvisioner

settings = JobProvisioningSettings(
    ingestion_group="sales",
    configuration_fqn="main.synchole.jdbc_ingestion_config",
    watermark_fqn="main.synchole.jdbc_ingestion_watermark",
    planner_notebook_path="notebooks/Task - Plan Configs",
    worker_notebook_path="notebooks/Task - Run Ingestion",
    wheel_uri="/Volumes/main/synchole/wheels/synchole-0.4.0-py3-none-any.whl",   # or any pip requirement
    foreach_concurrency=4,
    cdc_pipeline_file_path="/Workspace/Users/<user>/synchole/src/synchole/pipeline_bootstrap.py",
    # cdc_pipeline_id="<pipeline-uuid>",   # reuse an existing pipeline instead
    git_url="https://github.com/databricks-industry-solutions/synchole",   # empty = workspace notebook paths
    git_branch="main",
    # git_tag="v0.4.0",                    # or git_commit="<sha>": pin the notebooks instead of the branch
    # git_provider="gitHubEnterprise",     # only for a self-hosted Git server; detected otherwise
)

result = IngestionJobProvisioner().create_or_update_job(settings)
# {"job_id": ..., "pipeline_id": ..., "job_name": "[sales] Ingestion Pipeline"}
```

Or via the CLI:

```bash
synchole-create-job \
  --configuration-fqn main.synchole.jdbc_ingestion_config \
  --watermark-fqn main.synchole.jdbc_ingestion_watermark \
  --ingestion-group sales \
  --wheel-uri "/Volumes/main/synchole/wheels/synchole-0.4.0-py3-none-any.whl" \
  --planner-notebook-path "/Workspace/Users/<user>/synchole/notebooks/Task - Plan Configs" \
  --worker-notebook-path "/Workspace/Users/<user>/synchole/notebooks/Task - Run Ingestion" \
  --foreach-concurrency 4
```

Without `--git-url` the notebook paths are workspace paths. With it, they are paths in the repository, and `--git-branch` (default `main`), `--git-tag` or `--git-commit` picks the version; `--git-provider` is only needed for a self-hosted Git server. `--wheel-uri` accepts a wheel in a volume or the workspace, or any pip requirement such as `synchole==0.4.0`. `--cdc-pipeline-file-path` defaults to `./src/synchole/pipeline_bootstrap.py`.
