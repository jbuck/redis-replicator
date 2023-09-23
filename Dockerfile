FROM node:lts
COPY package*.json ./
RUN npm ci --production
COPY . .
