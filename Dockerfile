FROM node:lts
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .
