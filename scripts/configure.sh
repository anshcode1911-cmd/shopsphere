#!/usr/bin/env bash
# One-time: replace placeholders with your project / repo and create terraform.tfvars.
#   bash scripts/configure.sh <gcp-project-id> <github-owner>/<repo-name> [region] [zone]
set -euo pipefail
cd "$(dirname "$0")/.."

PROJECT=${1:?gcp project id}
REPO=${2:?github owner/repo}
REGION=${3:-us-central1}
ZONE=${4:-${REGION}-a}

# portable in-place sed (GNU + macOS)
sedi() { if sed --version >/dev/null 2>&1; then sed -i "$@"; else sed -i '' "$@"; fi; }

files=$(grep -rl --exclude-dir=.git -e YOUR_GCP_PROJECT -e YOUR_GITHUB_USER/shopsphere gitops)
for f in $files; do
  sedi "s#YOUR_GCP_PROJECT#${PROJECT}#g; s#https://github.com/YOUR_GITHUB_USER/shopsphere.git#https://github.com/${REPO}.git#g" "$f"
done
sedi "s#us-central1-docker.pkg.dev#${REGION}-docker.pkg.dev#g" gitops/apps/ecommerce/kustomization.yaml

cat > infra/terraform/01-platform/terraform.tfvars <<TF
project_id  = "${PROJECT}"
region      = "${REGION}"
zone        = "${ZONE}"
github_repo = "${REPO}"
TF

cat > infra/terraform/02-bootstrap/terraform.tfvars <<TF
gitops_repo_url = "https://github.com/${REPO}.git"
gitops_revision = "main"
TF

echo "Configured for project=${PROJECT} repo=${REPO} region=${REGION} zone=${ZONE}"
echo "Updated: ${files}"
