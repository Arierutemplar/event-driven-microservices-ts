# Política de seguridad

## Reportar una vulnerabilidad

No abras issues públicos para vulnerabilidades. Usa **Security → Report a vulnerability**
(GitHub Private Vulnerability Reporting) en este repositorio. Incluye versión/commit afectado,
pasos de reproducción e impacto. Se acusará recibo en un plazo razonable y se coordinará la
divulgación una vez publicada la corrección.

## Versiones soportadas

Solo la rama `main` y la última etiqueta `v*.*.*` reciben correcciones.

## Controles en el pipeline

- `govulncheck` (Go), `npm audit --audit-level=high` y Trivy (filesystem, secretos e imágenes)
  bloquean CI ante hallazgos alcanzables/HIGH/CRITICAL con corrección disponible.
- SBOM CycloneDX por imagen en CI; las imágenes publicadas en GHCR (`release.yml`) llevan SBOM,
  provenance SLSA de BuildKit y firma keyless de cosign por digest.
- Dependabot actualiza semanalmente módulos Go, npm, GitHub Actions (fijadas por SHA) e imágenes base.

Verificar una imagen publicada:

```bash
cosign verify ghcr.io/arierutemplar/event-driven-microservices-ts@sha256:<digest> \
  --certificate-identity-regexp 'https://github.com/Arierutemplar/event-driven-microservices-ts/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```
