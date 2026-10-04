# Self-hosted RelayHistory server. Build from the repository root:
#   docker build -f packages/server/Dockerfile -t relayhistory-server .
FROM node:22-bookworm-slim AS build
WORKDIR /src

COPY packages/engine/package.json packages/engine/package-lock.json packages/engine/
RUN npm ci --prefix packages/engine --no-audit --no-fund
COPY packages/engine packages/engine
RUN npm run --prefix packages/engine build

# `install-links` (packages/server/.npmrc) installs the engine as a packed copy, so the
# runtime image carries no source tree and one hono/drizzle-orm instance.
COPY packages/server/package.json packages/server/package-lock.json packages/server/.npmrc packages/server/
RUN cd packages/server && npm ci --no-audit --no-fund
COPY packages/server packages/server
RUN cd packages/server && npm run build && npm prune --omit=dev --no-audit --no-fund

FROM node:22-bookworm-slim
ENV NODE_ENV=production HOST=0.0.0.0
WORKDIR /app
COPY --from=build /src/packages/server/package.json ./
COPY --from=build /src/packages/server/node_modules ./node_modules
COPY --from=build /src/packages/server/dist ./dist
RUN chmod +x dist/cli.js && ln -s /app/dist/cli.js /usr/local/bin/relayhistory-server
USER node
EXPOSE 8080
# Start-up applies migrations and resumable projection backfills before listening; after a
# large upgrade that can take a while, and each step is bounded by statement timeouts.
HEALTHCHECK --interval=10s --timeout=3s --start-period=1h --retries=3 \
  CMD ["node", "-e", "fetch('http://127.0.0.1:'+(process.env.PORT||8080)+'/ready').then(r=>process.exit(r.ok?0:1),()=>process.exit(1))"]
ENTRYPOINT ["relayhistory-server"]
CMD ["serve"]
