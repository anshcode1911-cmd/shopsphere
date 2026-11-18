# ---------------------------------------------------------------------------
# Stage 02 — cluster bootstrap (still IaC, no manual kubectl):
#   1. namespace + infra-derived ConfigMap/Secret for the microservices
#   2. ArgoCD (Helm)
#   3. ArgoCD root "app-of-apps" Application -> everything else is GitOps
# ---------------------------------------------------------------------------
resource "kubernetes_namespace" "app" {
  metadata {
    name = var.app_namespace
    labels = {
      "app.kubernetes.io/part-of" = "shopsphere"
    }
  }
}

# Non-secret infrastructure endpoints produced by Terraform stage 01.
resource "kubernetes_config_map" "platform" {
  metadata {
    name      = "platform-config"
    namespace = kubernetes_namespace.app.metadata[0].name
  }
  data = {
    GCP_PROJECT         = local.p.project_id
    DB_HOST             = local.p.db_host
    DB_PORT             = "5432"
    DB_USER             = local.p.db_user
    FIRESTORE_DATABASE  = local.p.firestore_database
    PUBSUB_ORDER_TOPIC  = local.p.pubsub_order_topic
    KAFKA_BOOTSTRAP     = local.p.kafka_bootstrap
    KAFKA_SASL_USERNAME = local.p.kafka_sasl_username
    KAFKA_EVENTS_TOPIC  = "shop-events"
    KAFKA_RESULTS_TOPIC = "analytics-results"
    APP_GSA_EMAIL       = local.p.app_gsa_email
  }
}

resource "kubernetes_secret" "platform" {
  metadata {
    name      = "platform-secrets"
    namespace = kubernetes_namespace.app.metadata[0].name
  }
  data = {
    DB_PASSWORD         = local.p.db_password
    KAFKA_SASL_PASSWORD = local.p.kafka_sasl_password
  }
}

resource "helm_release" "argocd" {
  name             = "argocd"
  namespace        = "argocd"
  create_namespace = true
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  version          = "7.7.0"
  timeout          = 900

  values = [yamlencode({
    configs = {
      params = {
        "server.insecure" = true # UI reached through kubectl port-forward
      }
      cm = {
        "timeout.reconciliation" = "60s"
      }
    }
  })]
}

# Optional: credentials for a private GitOps repository
resource "kubernetes_secret" "repo" {
  count = var.git_token == "" ? 0 : 1
  metadata {
    name      = "gitops-repo"
    namespace = "argocd"
    labels = {
      "argocd.argoproj.io/secret-type" = "repository"
    }
  }
  data = {
    type     = "git"
    url      = var.gitops_repo_url
    username = var.git_username
    password = var.git_token
  }
  depends_on = [helm_release.argocd]
}

# Root Application (app-of-apps). It points at gitops/argocd/, which declares the
# child Applications: ecommerce services, kube-prometheus-stack, Loki, dashboards.
resource "helm_release" "root_app" {
  name       = "shopsphere-root"
  namespace  = "argocd"
  repository = "https://argoproj.github.io/argo-helm"
  chart      = "argocd-apps"
  version    = "2.0.0"

  values = [yamlencode({
    applications = {
      shopsphere-root = {
        namespace  = "argocd"
        project    = "default"
        finalizers = ["resources-finalizer.argocd.argoproj.io"]
        source = {
          repoURL        = var.gitops_repo_url
          targetRevision = var.gitops_revision
          path           = "gitops/argocd"
          directory = {
            recurse = false
          }
        }
        destination = {
          server    = "https://kubernetes.default.svc"
          namespace = "argocd"
        }
        syncPolicy = {
          automated = {
            prune    = true
            selfHeal = true
          }
        }
      }
    }
  })]

  depends_on = [helm_release.argocd, kubernetes_secret.repo]
}
