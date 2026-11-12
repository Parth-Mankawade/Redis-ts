FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build

FROM node:22-alpine
WORKDIR /app
COPY --from=build /app/dist ./dist
VOLUME /data
EXPOSE 6379
CMD ["node", "dist/index.js", "--port", "6379", "--dir", "/data"]
