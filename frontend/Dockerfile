# syntax=docker/dockerfile:1

FROM node:24-alpine AS build
WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
ARG VITE_WS_TOKEN
ARG VITE_INGEST_API_KEY
ARG VITE_TENANT_ID
ARG VITE_API_URL
ARG VITE_WS_URL
ARG VITE_WS_TICKET_URL
RUN npm run build

FROM nginxinc/nginx-unprivileged:alpine
# Apply Alpine security fixes published after the base image was built.
USER root
RUN apk upgrade --no-cache
USER 101
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
