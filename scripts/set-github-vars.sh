#!/usr/bin/env bash
# Copy Terraform outputs into GitHub Actions repository variables (needs `gh auth login`).
#   bash scripts/set-github-vars.sh <github-owner>/<repo-name>
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=${1:?github owner/repo}
tfo() { terraform -chdir=infra/terraform/01-platform output -raw "$1"; }

gh variable set GCP_PROJECT          --repo "$REPO" --body "$(tfo project_id)"
gh variable set GCP_REGION           --repo "$REPO" --body "$(tfo region)"
gh variable set GCP_WIF_PROVIDER     --repo "$REPO" --body "$(tfo ci_workload_identity_provider)"
gh variable set GCP_CI_SA            --repo "$REPO" --body "$(tfo ci_service_account)"
gh variable set FLINK_BUCKET         --repo "$REPO" --body "$(tfo flink_bucket)"
gh variable set DATAPROC_CLUSTER     --repo "$REPO" --body "$(tfo dataproc_cluster)"
gh variable set KAFKA_BOOTSTRAP      --repo "$REPO" --body "$(tfo kafka_bootstrap)"
gh variable set KAFKA_SASL_USERNAME  --repo "$REPO" --body "$(tfo kafka_sasl_username)"
echo "GitHub variables set on $REPO"
