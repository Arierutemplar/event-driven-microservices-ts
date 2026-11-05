#!/bin/sh
set -eu

export DATABASE_URL="${DATABASE_URL:-file:/app/order-service/data/orders.db}"

data_dir="$(dirname "${DATABASE_URL#file:}")"
mkdir -p "$data_dir"

cd /app
npx prisma db push --schema=order-service/prisma/schema.prisma --skip-generate
chown -R node:node "$data_dir"

exec su-exec node node order-service/dist/index.js
