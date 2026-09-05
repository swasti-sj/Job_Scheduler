# syntax=docker/dockerfile:1
FROM node:20-alpine AS base
WORKDIR /app
ENV NODE_ENV=production

FROM base AS deps
COPY package.json package-lock.json* ./
RUN npm ci --include=dev

FROM deps AS build
COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npx tsc -p tsconfig.build.json

FROM base AS runtime
# tini reaps zombies and, more importantly, forwards SIGTERM to node so the
# graceful shutdown path actually runs instead of the container being killed.
RUN apk add --no-cache tini
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=build /app/dist ./dist
COPY migrations ./migrations
USER node
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/bin/api.js"]
