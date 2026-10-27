FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
COPY user-service/package.json ./user-service/package.json
COPY order-service/package.json ./order-service/package.json
COPY notification-service/package.json ./notification-service/package.json
RUN npm ci --workspace=notification-service

COPY notification-service/tsconfig.json ./notification-service/tsconfig.json
COPY notification-service/src ./notification-service/src
RUN npm run build --workspace=notification-service

FROM node:20-alpine AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY package.json package-lock.json ./
COPY user-service/package.json ./user-service/package.json
COPY order-service/package.json ./order-service/package.json
COPY notification-service/package.json ./notification-service/package.json
RUN npm ci --omit=dev --workspace=notification-service && npm cache clean --force

COPY --from=build --chown=node:node /app/notification-service/dist ./notification-service/dist

USER node
CMD ["node", "notification-service/dist/index.js"]
