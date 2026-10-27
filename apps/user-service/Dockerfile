FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
COPY user-service/package.json ./user-service/package.json
COPY order-service/package.json ./order-service/package.json
COPY notification-service/package.json ./notification-service/package.json
RUN npm ci --workspace=user-service

COPY user-service/tsconfig.json ./user-service/tsconfig.json
COPY user-service/src ./user-service/src
COPY proto ./proto
RUN npm run build --workspace=user-service

FROM node:20-alpine AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY package.json package-lock.json ./
COPY user-service/package.json ./user-service/package.json
COPY order-service/package.json ./order-service/package.json
COPY notification-service/package.json ./notification-service/package.json
RUN npm ci --omit=dev --workspace=user-service && npm cache clean --force

COPY --from=build --chown=node:node /app/user-service/dist ./user-service/dist
COPY --from=build --chown=node:node /app/proto ./proto

USER node
EXPOSE 50051
CMD ["node", "user-service/dist/index.js"]
