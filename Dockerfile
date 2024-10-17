# Stage 1: Build Stage
FROM node:20-alpine AS builder
WORKDIR /src
COPY /package.json ./
RUN npm install --verbose --production
COPY /build /src

# Stage 2: Production Stage
FROM node:20-alpine
WORKDIR /src
ENV NODE_ENV=production
COPY --from=builder /src /src
CMD ["node", "index.js"]
