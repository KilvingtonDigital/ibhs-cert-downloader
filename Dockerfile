# Use Apify’s Playwright + Chrome base image
FROM apify/actor-node-playwright-chrome

# Copy package manifests and install dependencies first (better caching)
COPY package*.json ./
RUN npm ci --omit=dev

# ✅ Install the matching Playwright browsers for the version in your package-lock.json
# This downloads Chromium into the container image so it’s available at runtime.
RUN npx playwright install --with-deps chromium

# Copy app code and actor spec
COPY ./src ./src
COPY ./apify.json ./apify.json

# Start the actor
CMD ["npm", "start"]