# Kubernetes deployment notes

The base is a template, not an independently deployable production environment.
Create an overlay with the real immutable image digest, PostgreSQL host/user/database,
HTTPS origins and the **actual ingress-controller source CIDRs** in
`TRUSTED_PROXY_CIDRS`. Do not trust all pod or internet networks. Restrict backend
ingress at the network layer to the ingress controller and monitoring components,
and ensure the ingress overwrites forwarding headers and terminates TLS.

The namespace enforces Pod Security `restricted`; the backend runs as UID 10001 with a
read-only root filesystem, no capabilities and seccomp `RuntimeDefault`. `overlays/local`
relaxes enforcement to `baseline` only because its bundled PostgreSQL/Redis/NATS images
run as root. Tagging `v*.*.*` runs `.github/workflows/release.yml`, which publishes
`ghcr.io/arierutemplar/event-driven-microservices-ts`, signs it with cosign and prints the digest to
pin in the production overlay.

## Vault Agent or ESO

Choose **one** of the following integrations for a deployment. Both examples assume
Vault KV v2 mounted at `kv`, the secret `analytics/backend`, and five string properties:
`api_key`, `ws_ticket_secret` (at least 32 characters), `postgres_password`, `nats_url`
and `redis_url`. The base ConfigMap sets `WS_AUTH_MODE=ticket`: browsers obtain a
short-lived, tenant-bound ticket from `POST /api/ws-ticket` and no static WebSocket token
is deployed. The legacy `WS_TOKEN` is not used in Kubernetes. Store real
values in Vault outside this repository. Grant each Vault role only `read` on
`kv/data/analytics/backend`; bind it to the indicated Kubernetes service account,
namespace `analytics` and token audience `vault`. Configure Vault's Kubernetes auth
backend and TokenReview permissions according to your cluster and Vault deployment.
The application still requires the non-secret production settings described above.

### Vault Agent injection

`vault-agent/` is a Kustomize overlay using Vault Agent Injector and a projected
service-account token for audience `vault`. It removes the ESO `envFrom` reference,
renders five separate files readable by UID 10001, and supplies their `_FILE` paths to
the application. The agent init container fails on missing template keys; no Vault
token is shared with the application. This example uses init-only injection because
the backend reads secrets once at startup.

Install the Vault Agent Injector, configure its webhook and TLS to Vault, then replace
`vault.example.com`, the Vault role, mount path and any private CA configuration in
the overlay. Bind Vault role `analytics-agent` to service account
`analytics-vault-agent`. With a private Vault CA, configure the injector or mount a
CA using its `tls-secret` and `ca-cert` annotations; never disable TLS verification.
Inspect with `kubectl kustomize k8s/vault-agent` before applying. Coordinate a rolling
restart after rotation; the init-only agent does not refresh files.

### External Secrets Operator

The secret manifests in `eso/` are templates and are excluded from the base.
Install ESO, create namespace `analytics`, then apply `eso/serviceaccount.yaml`,
an adapted `eso/secretstore.example.yaml`, and `eso/externalsecret.example.yaml`. Bind Vault
role `analytics-eso` to service account `analytics-eso-vault`; ensure ESO is allowed
to request that service account's token. Set the real Vault URL and, for a private CA,
configure the SecretStore `caProvider` to reference a trusted CA ConfigMap. ESO creates
the Kubernetes Secret `analytics-secrets`, which the base deployment reads via
`envFrom`; Kubernetes Secret encryption and access controls must be configured by the
cluster operator. Wait for both `SecretStore` and `ExternalSecret` to report Ready
before deploying the backend. ESO refreshes the Kubernetes Secret every hour, but
environment variables do not update in running pods: roll out the deployment after
rotation.

For either option, add optional `TENANT_API_KEYS`, S3 credentials or OTLP headers only
when needed, using the same `_FILE` mechanism for Agent or extra ESO mappings. Never
commit rendered files, access tokens, or the resulting Kubernetes Secret. The examples
require an external Vault and were not applied to a live cluster.

`ingress.example.yaml` is not included in the base: replace the host/class and
provision `analytics-ingress-tls` through your certificate manager. Protect traffic
between the ingress and backend with mTLS at the service mesh or enable the backend's
`TLS_CERT_FILE`/`TLS_KEY_FILE` and configure HTTPS upstream and probes in an overlay.
Use `verify-full` for PostgreSQL, `tls://` for NATS and `rediss://` for Redis; mount
the corresponding CA and optional client certificates if private PKI is used.

Enable OTLP with a TLS collector endpoint and suitable egress/credentials in an
overlay. Ensure collector availability and sampling are appropriate for the load.
`kubectl kustomize k8s/base` validates rendering only; it does not provision external
services, ingress, secrets or prove high availability.

## Listo para portafolio vs. Requisitos de infraestructura real en producción

**Listo para portafolio (verificable sin infraestructura externa):** `kubectl kustomize`
renders `base`, `overlays/local`, `overlays/production-example`, `vault-agent` and `eso`;
the CI pipeline tests the backend against real PostgreSQL, Redis and NATS JetStream,
validates Prometheus rules/Alertmanager config and scans the images. The repository is
ready to validate as a deployment package and portfolio implementation. It does not
certify a live environment.

**Requisitos de infraestructura real en producción.** Before production, an
operator must:

- apply an immutable image digest and real namespace/service-account values;
- provision PostgreSQL, NATS, Redis and object storage with HA and backups;
- install and test Vault/ESO or another secret delivery mechanism;
- configure ingress, certificates, private CA bundles, proxy CIDRs and network policies;
- configure Alertmanager or an external alert receiver and test paging/recovery;
- run the k6 profiles and outage/recovery drills against representative infrastructure;
- configure artifact signing/attestation and CVE exception handling if required.

The local and production-example overlays, `kubectl kustomize`, Compose validation
and CI integration are evidence of configuration correctness; they do not replace
external infrastructure validation.
