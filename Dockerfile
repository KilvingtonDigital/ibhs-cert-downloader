FROM apify/actor-node-playwright-chrome

COPY package*.json ./
RUN npm ci --omit=dev

COPY ./src ./src
COPY ./apify.json ./apify.json

CMD ["npm", "start"]
