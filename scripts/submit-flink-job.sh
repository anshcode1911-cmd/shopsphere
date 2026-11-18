#!/usr/bin/env bash
# Submit (or re-submit) the Flink analytics job to the Dataproc cluster.
# Used by .github/workflows/flink-job.yaml, and can be run by hand:
#   export JAR=gs://<bucket>/jars/analytics-flink-<sha>.jar   (or build + upload first)
#   bash scripts/submit-flink-job.sh
# Missing variables are read from Terraform outputs of infra/terraform/01-platform.
set -euo pipefail
cd "$(dirname "$0")/.."

tfo() { terraform -chdir=infra/terraform/01-platform output -raw "$1"; }

GCP_PROJECT=${GCP_PROJECT:-$(tfo project_id)}
REGION=${REGION:-$(tfo region)}
CLUSTER=${CLUSTER:-$(tfo dataproc_cluster)}
FLINK_BUCKET=${FLINK_BUCKET:-$(tfo flink_bucket)}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-$(tfo kafka_bootstrap)}
KAFKA_USER=${KAFKA_USER:-$(tfo kafka_sasl_username)}
SECRET=${KAFKA_SECRET:-shopsphere-kafka-sasl-password}

if [[ -z "${JAR:-}" ]]; then
  echo ">> building jar locally"
  (cd analytics-flink && mvn -B -q package)
  JAR="gs://${FLINK_BUCKET}/jars/analytics-flink-manual-$(date +%s).jar"
  gcloud storage cp analytics-flink/target/analytics-flink-1.0.0.jar "$JAR"
fi

KAFKA_PASSWORD=$(gcloud secrets versions access latest --secret="$SECRET" --project="$GCP_PROJECT")

echo ">> cancelling previously running Flink jobs on $CLUSTER"
for id in $(gcloud dataproc jobs list --cluster="$CLUSTER" --region="$REGION" --project="$GCP_PROJECT" \
              --state-filter=active --filter="labels.app=shopsphere-analytics" --format="value(reference.jobId)"); do
  gcloud dataproc jobs kill "$id" --region="$REGION" --project="$GCP_PROJECT" --quiet || true
done

echo ">> submitting $JAR"
gcloud dataproc jobs submit flink \
  --project="$GCP_PROJECT" --region="$REGION" --cluster="$CLUSTER" \
  --jar="$JAR" \
  --labels=app=shopsphere-analytics \
  ${1:-} \
  -- \
  --bootstrap "$KAFKA_BOOTSTRAP" \
  --username "$KAFKA_USER" \
  --password "$KAFKA_PASSWORD" \
  --input shop-events --output analytics-results --window-seconds 60 \
  --checkpoint-dir "hdfs:///flink/checkpoints/shopsphere"
