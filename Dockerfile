FROM node:14-stretch AS development

WORKDIR /usr/src/app
COPY package*.json ./

RUN npm install
COPY . .

FROM node:14-stretch AS production

ARG NODE_ENV=production
ENV NODE_ENV=${NODE_ENV}

WORKDIR /usr/src/app
COPY package*.json ./

RUN npm update -g yarn npm
RUN yarn install --production

COPY . .

CMD ["node --max-old-space-size=4096", "index.js"]
