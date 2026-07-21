FROM node:24-slim AS builder

WORKDIR /app
RUN corepack enable

# Copy workspace manifests first so dependency installation stays cached while source changes.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/protocol/package.json packages/protocol/package.json
COPY packages/server/package.json packages/server/package.json
COPY packages/live-collection/package.json packages/live-collection/package.json
COPY packages/react/package.json packages/react/package.json
COPY examples/playground/package.json examples/playground/package.json
COPY examples/pi-demo/shared/package.json examples/pi-demo/shared/package.json
COPY examples/pi-demo/server/package.json examples/pi-demo/server/package.json
COPY examples/pi-demo/web/package.json examples/pi-demo/web/package.json
RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm --filter @triargos/live-collection-protocol build \
  && pnpm --filter @triargos/live-collection-server build \
  && pnpm --filter @triargos/live-collection build \
  && pnpm --filter @triargos/live-collection-react build \
  && pnpm --filter @pi-demo/web exec vite build

FROM node:24-slim AS runner

WORKDIR /app
RUN corepack enable
COPY --from=builder /app /app

ENV NODE_ENV=production
ENV PORT=3000
ENV STATIC_DIR=/app/examples/pi-demo/web/dist
ENV PORT=3000

CMD ["pnpm", "--filter", "@pi-demo/server", "start"]
