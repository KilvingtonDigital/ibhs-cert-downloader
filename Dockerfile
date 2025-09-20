# Dockerfile
FROM apify/actor-node-playwright-chrome

# Install JS deps
COPY package*.json ./
RUN npm ci --omit=dev

# Make sure browser download runs as non-root user
USER myuser
RUN npx playwright install chromium

# Copy app
COPY ./src ./src
COPY ./apify.json ./apify.json

CMD ["npm", "start"]
