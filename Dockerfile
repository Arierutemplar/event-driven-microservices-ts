# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/dlqctl ./cmd/dlqctl

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
RUN addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
COPY --from=build /out/api /usr/local/bin/api
COPY --from=build /out/dlqctl /usr/local/bin/dlqctl

# UID numérico: coincide con runAsUser de k8s y permite verificar runAsNonRoot.
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["api"]
