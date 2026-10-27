FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
COPY user-service/package.json ./user-service/package.json
COPY order-service/package.json ./order-service/package.json
COPY notification-service/package.json ./notification-service/package.json
RUN npm ci --workspace=order-service

COPY order-service/tsconfig.json ./order-service/tsconfig.json
COPY order-service/prisma ./order-service/prisma
COPY order-service/src ./order-service/src
COPY proto ./proto
RUN npx prisma generate --schema=order-service/prisma/schema.prisma
RUN npm run build --workspace=order-service

FROM node:20-alpine AS runtime

ENV NODE_ENV=production
WORKDIR /app

COPY package.json package-lock.json ./
COPY user-service/package.json ./user-service/package.json
COPY order-service/package.json ./order-service/package.json
COPY notification-service/package.json ./notification-service/package.json
COPY order-service/prisma ./order-service/prisma
RUN npm ci --omit=dev --workspace=order-service && npm cache clean --force

COPY --from=build --chown=node:node /app/node_modules/.prisma/client ./node_modules/.prisma/client
COPY --from=build --chown=node:node /app/order-service/dist ./order-service/dist
COPY --from=build --chown=node:node /app/proto ./proto

USER node
EXPOSE 3000
CMD ["node", "order-service/dist/index.js"]
