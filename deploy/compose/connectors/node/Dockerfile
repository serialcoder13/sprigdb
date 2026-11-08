# Node mysql2 (version pinned in package.json / package-lock.json).
FROM node:22.21.1-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY check.js ./
ENTRYPOINT ["node", "/app/check.js"]
