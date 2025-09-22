// src/main.js - Simplified Version (Apify Storage Only)
import { Actor, log } from 'apify';
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (base, spread = 350) => base + Math.floor(Math.random() * spread);

// === ENHANCED DEBUG FUNCTIONS ===
async function debugPageState(page, step) {
  const timestamp = new Date().toISOString();
  log.info(`🔍 === DEBUG ${step} at ${timestamp} ===`);
  
  try {
    const url = page.url();
    const title = await page.title();
    log.info(`📍 URL: ${url}`);
    log.info(`📄 Title: ${title}`);
    
    const loginElements = await page.locator('input[type="email"], input[type="password"], [text*="sign in" i], [text*="log in" i]').count();
    log.info(`🔐 Login elements found: ${loginElements}`);
    
    const searchElements = await Promise.all([
      page.locator('input[type="search"]').count(),
      page.locator('[placeholder*="search" i]').count(),
      page.locator('[aria-label*="search" i]').count(),
      page.locator('input').count(),
      page.locator('button, [role="button"]').count(),
    ]);
    
    log.info(`🔍 Search elements - type=search: ${searchElements[0]}, placeholder: ${searchElements[1]}, aria-label: ${searchElements[2]}`);
    log.info(`📝 Total inputs: ${searchElements[3]}, Total buttons: ${searchElements[4]}`);
    
    // Take screenshot for important steps
    const png = await page.screenshot({ fullPage: true });
    const screenshotKey = `debug-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.png`;
    await Actor.setValue(screenshotKey, png, { contentType: 'image/png' });
    log.info(`📸 Screenshot saved as: ${screenshotKey}`);
    
  } catch (e) {
    log.error(`❌ Debug failed: ${e.message}`);
  }
  
  log.info(`🔍 === END DEBUG ${step} ===`);
}

// Utility functions
const normalizeAddress = (s = '') =>
  s
    .toLowerCase()
    .replace(/[.,]/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\b(apt|apartment|ste|suite|unit)\b\s*\w+/g, '')
    .trim();

function sanitizeFileName(name) {
  return name.replace(/[\\/:*?"<>|]+/g, '_').slice(0, 180);
}

function kvSafeKey(name) {
  return (name || '')
    .replace(/[^a-zA-Z0-9!\-_\.'()]+/g, '-')
    .slice(0, 250);
}

async function streamToBuffer(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks);
}

// === LOGIN FUNCTION ===
async function ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs, debug }) {
  const emailSel = 'input[type="email"], input[name="email"], input[autocomplete="username"]';
  const passSel = 'input[type="password"], input[name="password"], input[autocomplete="current-password"]';
  const submitSel = 'button:has-text("Sign in"), button:has-text("Log in"), button[type="submit"], [type="submit"]';

  log.info('🔑 Starting login process...');
  await page.goto(loginUrl, { waitUntil: 'domcontentloaded', timeout: 90_000 });
  await page.waitForLoadState('networkidle', { timeout: 60_000 });
  
  if (debug) await debugPageState(page, 'LOGIN_PAGE_LOADED');

  if (await page.locator(emailSel).count()) {
    log.info('📧 Filling email field...');
    await page.fill(emailSel, username);
    await sleep(jitter(200));
  }
  
  if (await page.locator(passSel).count()) {
    log.info('🔒 Filling password field...');
    await page.fill(passSel, password);
    await sleep(jitter(200));
  }

  log.info('🔘 Clicking login button...');
  if (await page.locator(submitSel).count()) {
    await page.click(submitSel);
  } else if (await page.locator(passSel).count()) {
    await page.press(passSel, 'Enter');
  }

  log.info('⏳ Waiting for login to complete...');
  const loginGone = page.waitForSelector(emailSel, { state: 'detached', timeout: 30_000 }).catch(() => null);
  const appHint = page.waitForSelector('text=/Certificates?|Search/i', { timeout: 30_000 }).catch(() => null);
  await Promise.race([loginGone, appHint]);

  if (await page.locator(emailSel).count()) {
    if (await page.locator(submitSel).count()) await page.click(submitSel);
    await page.waitForLoadState('networkidle', { timeout: 30_000 });
  }

  if (debug) await debugPageState(page, 'POST_LOGIN');
  
  if (await page.locator(emailSel).count()) {
    throw new Error('Login did not complete (email field still visible). Check credentials.');
  }

  log.info('✅ Login successful!');
  await sleep(jitter(politeDelayMs));
}

// === SIMPLIFIED SEARCH AND EXTRACT FUNCTION ===
async function searchAndExtractCertificate(page, address, { politeDelayMs, debug }) {
  const key = normalizeAddress(address);
  log.info(`🎯 Processing address: ${address}`);

  // SEARCH PHASE
  if (debug) await debugPageState(page, 'BEFORE_SEARCH');

  let searchField = null;
  const strategies = [
    () => page.locator('input[type="search"]').first(),
    () => page.locator('[placeholder*="search" i]').first(),
    () => page.locator('[aria-label*="search" i]').first(),
    () => page.getByRole('searchbox').first(),
    () => page.getByRole('textbox').first(),
    () => page.locator('input').first()
  ];

  for (let i = 0; i < strategies.length; i++) {
    try {
      log.info(`🔍 Trying search strategy ${i + 1}...`);
      const element = strategies[i]();
      const count = await element.count();
      
      if (count > 0) {
        const isVisible = await element.isVisible();
        const isEnabled = await element.isEnabled();
        
        if (isVisible && isEnabled) {
          searchField = element;
          log.info(`✅ Using search strategy ${i + 1}`);
          break;
        }
      }
    } catch (e) {
      continue;
    }
  }

  if (!searchField) {
    throw new Error('No usable search field found');
  }

  // Perform search
  log.info(`📝 Searching for: ${address}`);
  await searchField.click();
  await page.waitForTimeout(500);
  await searchField.fill(address);
  await sleep(jitter(500));
  await page.keyboard.press('Enter');
  await page.waitForLoadState('networkidle', { timeout: 60_000 });
  await sleep(jitter(politeDelayMs));

  if (debug) await debugPageState(page, 'AFTER_SEARCH');

  // Wait for results and check if we found anything
  await page.waitForTimeout(3000);
  
  const resultsCount = await page.locator('tr, [role="row"]').count();
  log.info(`Found ${resultsCount} rows in results`);
  
  if (resultsCount <= 1) {
    throw new Error('No search results found for this address');
  }

  // Look for download button in results - sometimes it's directly available
  await debugPageState(page, 'CHECKING_FOR_DOWNLOAD');
  
  let downloadButton = page.locator('text=/download/i, [title*="download" i], button:has-text("Download")').first();
  
  if (!(await downloadButton.count()) || !(await downloadButton.isVisible())) {
    log.info('⚠️ No immediate download button found, trying to click on result...');
    
    // Try to click on something in the results to get to the detail view
    const streetNumber = address.split(',')[0].trim();
    const clickTargets = [
      page.locator(`text="${streetNumber}"`).first(),
      page.locator('tr').nth(1).locator('td').first(),
      page.locator('tr').nth(1),
      page.locator('a').first()
    ];
    
    let clickSuccessful = false;
    for (const target of clickTargets) {
      try {
        if (await target.count() > 0 && await target.isVisible()) {
          log.info(`🎯 Attempting to click on result element...`);
          await target.click({ timeout: 10000 });
          await page.waitForLoadState('networkidle', { timeout: 30000 });
          clickSuccessful = true;
          break;
        }
      } catch (e) {
        log.info(`Click attempt failed: ${e.message}`);
        continue;
      }
    }
    
    if (!clickSuccessful) {
      throw new Error('Could not click on any result element to access certificate details');
    }
    
    await debugPageState(page, 'AFTER_CLICKING_RESULT');
    
    // Now look for download button again
    downloadButton = page.locator('text=/download/i, [title*="download" i], button:has-text("Download")').first();
  }

  // Check for certificate tab/section
  const certTab = page.locator('[role="tab"], button, a').filter({ hasText: /certificate/i }).first();
  if (await certTab.count() && await certTab.isVisible()) {
    log.info('🏛️ Found certificate tab, clicking it...');
    await certTab.click();
    await page.waitForLoadState('networkidle', { timeout: 30000 });
    await sleep(jitter(300));
    
    // Update download button reference
    downloadButton = page.locator('text=/download/i, [title*="download" i], button:has-text("Download")').first();
  }

  await debugPageState(page, 'READY_FOR_DOWNLOAD');

  if (!(await downloadButton.count()) || !(await downloadButton.isVisible())) {
    throw new Error('Download button not found after navigating to certificate section');
  }

  // EXTRACT CERTIFICATE DETAILS
  const certificateData = {
    address: address,
    searchKey: key,
    timestamp: new Date().toISOString(),
    status: 'found',
    certificateDetails: {},
    apifyStorageInfo: null
  };

  // Try to extract some details from the page
  try {
    // Look for certificate ID
    const certIdText = await page.locator('text=/FH\\d+/').first().textContent().catch(() => '');
    if (certIdText) {
      certificateData.certificateDetails.certificateId = certIdText.trim();
    }

    // Look for designation
    const designationText = await page.locator('text=/roof/i, text=/wind/i, text=/impact/i').first().textContent().catch(() => '');
    if (designationText) {
      certificateData.certificateDetails.designation = designationText.trim();
    }

    // Look for expiration info
    const expText = await page.locator('text=/expire/i').first().textContent().catch(() => '');
    if (expText) {
      certificateData.certificateDetails.expirationInfo = expText.trim();
    }

  } catch (e) {
    log.warning(`Could not extract certificate details: ${e.message}`);
  }

  // DOWNLOAD THE CERTIFICATE
  log.info('📥 Starting certificate download...');
  
  // Set up download handlers
  const downloadPromise = page.waitForEvent('download', { timeout: 60_000 }).then(d => ({ kind: 'download', d })).catch(() => null);
  const responsePromise = page.waitForResponse(
    resp => (resp.headers()['content-type'] || '').toLowerCase().includes('application/pdf'),
    { timeout: 60_000 }
  ).then(r => ({ kind: 'response', r })).catch(() => null);

  await downloadButton.click();
  log.info('🖱️ Clicked download button, waiting for download...');

  const signal = await Promise.race([downloadPromise, responsePromise]);
  
  if (!signal) {
    throw new Error('Download did not start within 60 seconds');
  }

  let buffer = null;
  if (signal.kind === 'download') {
    log.info('📂 Processing download event...');
    try {
      const stream = await signal.d.createReadStream();
      buffer = stream ? await streamToBuffer(stream) : await fs.readFile(await signal.d.path());
    } catch (e) {
      log.warning(`Download stream failed, trying file path: ${e.message}`);
      buffer = await fs.readFile(await signal.d.path());
    }
  } else if (signal.kind === 'response') {
    log.info('📄 Processing PDF response...');
    buffer = await signal.r.body();
  }

  if (!buffer || buffer.length === 0) {
    throw new Error('Downloaded file is empty');
  }

  log.info(`✅ Certificate downloaded successfully: ${buffer.length} bytes`);

  // SAVE TO APIFY STORAGE
  const fileName = sanitizeFileName(`IBHS_Certificate_${key}_${Date.now()}.pdf`);
  const kvName = kvSafeKey(`${key}-certificate.pdf`);
  
  await Actor.setValue(kvName, buffer, { contentType: 'application/pdf' });
  log.info(`💾 Saved to Apify storage: ${kvName}`);

  // Create download URL for n8n to use
  const apifyStorageUrl = `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${kvName}`;
  
  certificateData.apifyStorageInfo = {
    keyValueStoreId: Actor.getEnv().defaultKeyValueStoreId,
    key: kvName,
    fileName: fileName,
    downloadUrl: apifyStorageUrl,
    fileSize: buffer.length,
    contentType: 'application/pdf',
    savedAt: new Date().toISOString()
  };

  return certificateData;
}

// === MAIN FUNCTION ===
async function run() {
  await Actor.init();

  const input = (await Actor.getInput()) || {};
  const {
    loginUrl = 'https://app.ibhs.org/fh',
    address = '',
    politeDelayMs = 800,
    debug = false,
    username: usernameFromInput,
    password: passwordFromInput,
  } = input;

  // Handle both single address and legacy addresses array
  let targetAddress = address;
  if (!targetAddress && input.addresses) {
    if (Array.isArray(input.addresses)) {
      targetAddress = input.addresses[0];
    } else if (typeof input.addresses === 'string') {
      targetAddress = input.addresses.split(/\r?\n/)[0].trim();
    }
  }

  if (!targetAddress) {
    throw new Error('No address provided. Use "address" field.');
  }

  const username = usernameFromInput || process.env.IBHS_USERNAME;
  const password = passwordFromInput || process.env.IBHS_PASSWORD;

  if (!username || !password) {
    throw new Error('Missing credentials. Set IBHS_USERNAME and IBHS_PASSWORD environment variables.');
  }

  log.info(`🚀 Starting IBHS certificate search for: ${targetAddress}`);

  const browser = await chromium.launch({
    headless: !debug,
    slowMo: debug ? 250 : 0,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
  });

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36',
    acceptDownloads: true,
  });

  const page = await context.newPage();

  try {
    // Login
    await ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs, debug });

    // Search and extract certificate
    const result = await searchAndExtractCertificate(page, targetAddress, { politeDelayMs, debug });
    
    log.info(`🎉 Successfully processed: ${targetAddress}`);
    log.info(`📁 PDF available at: ${result.apifyStorageInfo.downloadUrl}`);
    
    // Push structured data to dataset for n8n
    await Actor.pushData(result);
    log.info('📊 Result data pushed to dataset');

    // Also save to key-value store for easy access
    await Actor.setValue('latest-result', result);
    
    return result;

  } catch (error) {
    log.error(`❌ Failed to process ${targetAddress}: ${error.message}`);
    
    const errorResult = {
      address: targetAddress,
      searchKey: normalizeAddress(targetAddress),
      timestamp: new Date().toISOString(),
      status: 'failed',
      error: error.message,
      certificateDetails: null,
      apifyStorageInfo: null
    };

    await Actor.pushData(errorResult);
    await Actor.setValue('latest-result', errorResult);
    
    throw error;
  } finally {
    await browser.close();
    await Actor.exit();
  }
}

Actor.main(run);
