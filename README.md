# ShopSphere — cloud-native e-commerce on Google Cloud

CS/SS G527 Cloud Computing assignment. The project has 8 microservices on GCP and uses the **Saga pattern** for checkout. Everything is wired with Terraform, ArgoCD, Kafka + Flink, Prometheus/Grafana/Loki and Cloud Logging.

Full design: [`docs/DESIGN.md`](docs/DESIGN.md) (PDF: `docs/ShopSphere-Design-Document.pdf`). Task division: [`task_division.txt`](task_division.txt) (individual submission: Ansh, 2025H1120161P).

```
shopsphere/
├── proto/ecommerce.proto            gRPC contract (CatalogService, PaymentService)
├── libs/common/                     shared logging / metrics / kafka / db helpers
├── services/
│   ├── gateway/                     public web UI + API gateway (REST, gRPC client, Kafka producer)
│   ├── user-service/                REST + Cloud SQL
│   ├── catalog-service/             gRPC + Cloud SQL (inventory, saga step 1)
│   ├── order-service/               REST + Cloud SQL, Saga orchestrator (saga.py)
│   ├── payment-service/             gRPC + Firestore (saga step 2)
│   └── analytics-service/           Kafka consumer + Firestore + REST
├── functions/invoice-function/      Cloud Function gen2 (Pub/Sub-triggered, serverless)
├── analytics-flink/                 Flink job (Java) for Dataproc: Kafka -> 1-min window -> Kafka
├── infra/terraform/01-platform/     ALL GCP infrastructure
├── infra/terraform/02-bootstrap/    namespace, config/secrets, ArgoCD, root app
├── gitops/argocd/                   ArgoCD child Applications (app-of-apps)
├── gitops/apps/ecommerce/           Deployments, Services, HPAs, PDBs, ServiceMonitor (Kustomize)
├── gitops/apps/observability/       Grafana dashboard
├── .github/workflows/               CI: images + GitOps tag bump; Flink build/submit; terraform check
├── loadtest/k6-load.js              sustained load to demonstrate the HPAs
├── scripts/                         configure.sh, set-github-vars.sh, submit-flink-job.sh
├── tests/test_saga.py               saga unit tests
└── docs/                            design document + diagrams
```

---

## 0. Prerequisites (on your laptop)

| Tool | Install (macOS) |
|---|---|
| Google Cloud SDK + GKE auth plugin | `brew install --cask google-cloud-sdk` then `gcloud components install gke-gcloud-auth-plugin kubectl` |
| Terraform ≥ 1.6 | `brew tap hashicorp/tap && brew install hashicorp/tap/terraform` |
| GitHub CLI | `brew install gh` then `gh auth login` |
| k6 | `brew install k6` (or use Docker: `grafana/k6`) |

You also need a GCP project with **billing enabled** (the free-trial credits are enough) and you must be **Owner** of it.

## 1. Put the code on GitHub

```bash
cd shopsphere
gh repo create shopsphere --public --source=. --remote=origin   # public = ArgoCD needs no credentials
```

## 2. Point the repo at your project

```bash
export PROJECT_ID=<your-gcp-project-id>
export REPO=<your-github-user>/shopsphere
bash scripts/configure.sh $PROJECT_ID $REPO          # optional 3rd arg: region (default us-central1)
git add -A && git commit -m "configure project" && git push -u origin main
```

> The first push also starts the `build-and-deploy` workflow. It fails because the GitHub variables don't exist yet. That's expected; it is re-run in step 5.

## 3. Log in to Google Cloud

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project $PROJECT_ID
```

## 4. Provision the infrastructure (Terraform stage 01, about 30–40 minutes)

```bash
cd infra/terraform/01-platform
terraform init
terraform apply          # type "yes". The Managed Kafka cluster alone takes ~25-30 min.
cd ../../..
```

This creates the VPC/NAT, GKE, Cloud SQL, Firestore, buckets, Pub/Sub, Managed Kafka + topics, Dataproc (Flink), the Cloud Function, Artifact Registry, IAM/Workload Identity, CI federation, and the central log bucket.

If the apply fails partway through (for example, an API that was only just enabled hasn't propagated yet), run `terraform apply` again; it continues from where it stopped.

## 5. Connect CI and build everything

```bash
bash scripts/set-github-vars.sh $REPO
gh workflow run build-and-deploy --repo $REPO     # builds 6 images, commits new tags to gitops/
gh workflow run flink-analytics-job --repo $REPO  # builds + tests the Flink jar, submits it to Dataproc
gh run watch --repo $REPO                         # wait until both are green
git pull                                           # fetch the tag-bump commit CI made
```

## 6. Bootstrap the cluster (Terraform stage 02: ArgoCD + root app)

```bash
cd infra/terraform/02-bootstrap
terraform init
terraform apply
cd ../../..
gcloud container clusters get-credentials shopsphere-gke --zone us-central1-a
```

From here on, **ArgoCD deploys everything from Git**: the services, the HPAs, Prometheus/Grafana and Loki. Give it about 5 minutes.

```bash
kubectl get applications -n argocd             # all should become Synced / Healthy
kubectl get pods -n ecommerce
kubectl get svc gateway -n ecommerce           # EXTERNAL-IP = your public URL
```

Open `http://<EXTERNAL-IP>/` and you should see the store.

## 7. Open the dashboards

```bash
# ArgoCD UI  -> http://localhost:8080  (user: admin)
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' | base64 -d; echo
kubectl -n argocd port-forward svc/argocd-server 8080:80

# Grafana -> http://localhost:3000  (admin / shopsphere-admin)
#            Dashboards -> "ShopSphere — Services, HPA & Cluster Health"
kubectl -n monitoring port-forward svc/kps-grafana 3000:80

# Flink UI: Cloud Console -> Dataproc -> Clusters -> shopsphere-flink -> Web Interfaces -> YARN -> Flink
```

## 8. Demo script

**Saga (f) + REST/gRPC (b)** — in the web UI with User ID `u-0001`:

1. Add two products and click **Checkout**. The order is **COMPLETED** and the saga log shows 3 SUCCEEDED steps.
2. Tick **simulate payment failure** and check out again. The order is **CANCELLED**, the log shows `charge_payment FAILED` → `reserve_stock COMPENSATE SUCCEEDED`, and the product's stock is unchanged.
3. Add *Limited Edition Sneakers* 5 times (only 3 in stock) and check out. The order is **REJECTED** at step 1.

The same flow with curl:

```bash
GW=http://<EXTERNAL-IP>
curl -s -X POST $GW/api/orders -H 'Content-Type: application/json' \
  -d '{"user_id":"u-0001","items":[{"product_id":"p-001","quantity":1}],"simulate_payment_failure":true}' | jq
```

**Serverless (b) + storage (g)** — after a completed order:

```bash
gcloud storage ls -r gs://$PROJECT_ID-shopsphere-invoices/invoices/   # invoice written by the function
gcloud functions logs read invoice-function --region us-central1 --gen2 --limit 20
```

Firestore console → database `shopsphere` → collections `payments`, `invoices`, `notifications`, `analytics_windows`. Cloud SQL → `shopsphere-pg` (databases users, catalog, orders).

**Flink (e)** — browse and order for about 2 minutes. The **Live analytics** panel in the UI then fills with 1-minute windows. To read the results topic directly:

```bash
curl -s $GW/api/analytics/latest | jq '.windows[:5]'
gcloud dataproc jobs list --cluster shopsphere-flink --region us-central1 --state-filter active
```

**HPA + load test (c)** — use two terminals:

```bash
kubectl get hpa -n ecommerce -w                              # terminal 1
k6 run -e BASE_URL=$GW loadtest/k6-load.js                   # terminal 2 (about 10 min)
```

The gateway, order-service and catalog-service replicas climb as CPU passes 60 % (also visible in the Grafana *HPA current vs desired* panel), and scale back in after the test.

**GitOps (d)** — change something visible, for example the title text in `services/gateway/static/index.html`, then commit and push. CI builds the image and commits a new tag. ArgoCD shows the `shopsphere-ecommerce` app syncing and the pods roll. You can also show self-heal: `kubectl scale deploy user-service -n ecommerce --replicas=0` gets reverted.

**IaC (a)** — `terraform state list` in both stages shows every resource. `terraform plan` shows no drift.

**Observability (h)** — in the Grafana dashboard, show RPS, error rate and latency per service, cluster health, the saga panel and the Loki logs panel. In Cloud Logging → Log Analytics / Logs Explorer, choose the **`shopsphere-central`** log bucket (scope: *Log views*). It contains the GKE services, the Dataproc Flink job and the Cloud Function logs together. Example query:

```
resource.type="cloud_dataproc_job" OR jsonPayload.service="order-service"
```

## 9. Tear down (stop the billing)

```bash
cd infra/terraform/02-bootstrap && terraform destroy
cd ../01-platform && terraform destroy
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Error 403 … API not enabled` during the first apply | Wait 1–2 minutes and run `terraform apply` again |
| Managed Kafka / Dataproc unavailable in your region | Use `us-central1` (the default) |
| Pods stuck in `ImagePullBackOff` | CI hasn't pushed images yet. Run step 5 and check that `gitops/apps/ecommerce/kustomization.yaml` has SHA tags |
| `gitops-bump` job can't push | Repo → Settings → Actions → General → Workflow permissions → **Read and write** |
| GitHub auth step fails with `unauthorized_client` | `GCP_WIF_PROVIDER` / `GCP_CI_SA` vars missing, or the repo name differs from `github_repo` in tfvars |
| ArgoCD app `shopsphere-ecommerce` complains about `ServiceMonitor` | The monitoring app installs the CRD first; it auto-retries within a few minutes |
| Analytics panel stays empty | Check that the Flink job is running (`gcloud dataproc jobs list …`) and that there is traffic. Windows close 1 min + 10 s after the events |
| Cloud Function build error about permissions | Re-run `terraform apply` (the IAM grants for the build SA can take a minute to propagate) |

## Running locally (no cloud)

You only need PostgreSQL. Set `KAFKA_ENABLED=false PUBSUB_ENABLED=false`, point `DB_HOST/DB_USER/DB_PASSWORD/DB_NAME` at a local database, and use `CATALOG_GRPC_ADDR=dns:///localhost:50051`. The payment- and analytics-services need Firestore (use the emulator: `gcloud emulators firestore start`, then `export FIRESTORE_EMULATOR_HOST=…`). Unit tests: `pip install pytest && pytest tests/`.
