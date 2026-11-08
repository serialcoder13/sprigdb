# Prisma ORM 6 (versions pinned in package.json / package-lock.json). npm ci
# downloads Prisma's engines for this platform (linux-musl, OpenSSL 3); the
# client is generated at run time from the introspected schema.
FROM node:22.21.1-alpine
RUN apk add --no-cache openssl
WORKDIR /app
ENV CHECKPOINT_DISABLE=1 PRISMA_HIDE_UPDATE_MESSAGE=1
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && /app/node_modules/.bin/prisma --version
COPY check.js ./
ENTRYPOINT ["node", "/app/check.js"]
