# Use Apify’s Playwright+Chrome image (browsers already baked in)
FROM apify/actor-node-playwright-chrome

# IMPORTANT: Use the preinstalled browsers, don’t download again
ENV PLAYWRIGHT_BROWSERS_PATH=/usr/lib/playwright
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Install app deps
COPY package*.json ./
RUN npm ci --omit=dev

# App code + actor spec
COPY ./src ./src
COPY ./apify.json ./apify.json

# Start
CMD ["npm", "start"]
