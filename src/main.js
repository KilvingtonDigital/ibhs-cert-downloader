// src/main.js - IMPROVED VERSION
import { Actor, log } from 'apify';
import { chromium } from 'playwright';
import fs from 'fs/promises';
import { google } from 'googleapis';
import { Readable } from 'stream';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (base, spread = 350) => base + Math.floor(Math.random() * spread);

// ==================== UTILITY FUNCTIONS ====================

const normalizeAddress = (s = '') =>
  s.toLowerCase()
    .replace(/[.,]/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\b(apt|apartment|ste|suite|unit)\b\s*\w+/g, '')
    .trim();

function sanitizeFileName(name) {
  return name.replace(/[\\/:*?"<>|\s]+/g, '_').slice(0, 180);
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

// ==================== SCREENSHOT CAPTURE ====================

/**
 * ALWAYS capture a screenshot and save it to KVS
 * Returns the URL to access the screenshot
 */
async function captureAndSaveScreenshot(page, address, stage = 'final') {
  try {
    const timestamp = Date.now();
    const safeName = sanitizeFileName(address);
    const screenshotKey = `screenshot-${safeName}-${stage}-${timestamp}.png`;
    
    log.info(`📸 Capturing ${stage} screenshot for: ${address}`);
    
    // Full page screenshot
    const png = await page.screenshot({ 
      fullPage: true,
      timeout: 30000 
    });
    
    // Save to Key-Value Store
    await Actor.setValue(screenshotKey, png, { contentType: 'image/png' });
    
    const kvStoreId = Actor.getEnv().defaultKeyValueStoreId;
    const screenshotUrl = `https://api.apify.com/v2/key-value-stores/${kvStoreId}/records/${screenshotKey}`;
    
    log.info(`✅ Screenshot saved: ${screenshotKey}`);
    log.info(`🔗 Screenshot URL: ${screenshotUrl}`);
    
    return {
      key: screenshotKey,
      url: screenshotUrl,
      timestamp: new Date().toISOString()
    };
  } catch (e) {
    log.error(`❌ Screenshot capture failed: ${e.message}`);
    return {
      key: null,
      url: null,
      error: e.message,
      timestamp: new Date().toISOString()
    };
  }
}

// ==================== DATA EXTRACTION ====================

/**
 * Enhanced extraction with multiple strategies and fallbacks
 */
async function extractCertificateData(page) {
  const data = {
    fhNumber: null,
    approvedAt: null,
    expirationDate: null,
    buildingAddress: null,
    extractionAttempts: {
      fhNumber: [],
      approvedAt: [],
      expirationDate: [],
      buildingAddress: []
    }
  };

  try {
    log.info('📋 Starting comprehensive data extraction...');
    
    // Wait for content to stabilize
    await page.waitForTimeout(3000);
    
    // Get all page text
    const fullText = await page.textContent('body');
    const normalizedText = fullText.replace(/\s+/g, ' ').trim();
    
    // === EXTRACT FH/FEH NUMBER ===
    log.info('🔢 Extracting FH/FEH Number...');
    
    // Strategy 1: Direct text pattern
    const fhPatterns = [
      /FE?H\d{8,}/gi,
      /FE?H-?\d{8,}/gi,
      /\b(FH|FEH)[\s-]?(\d{8,})/gi
    ];
    
    for (const pattern of fhPatterns) {
      const matches = normalizedText.match(pattern);
      if (matches && matches.length > 0) {
        const fhNum = matches[0].replace(/\s|-/g, '').toUpperCase();
        data.extractionAttempts.fhNumber.push({ strategy: 'text_pattern', value: fhNum });
        if (!data.fhNumber) data.fhNumber = fhNum;
      }
    }
    
    // Strategy 2: DOM elements
    const fhSelectors = [
      'text=/FE?H\\d+/i',
      '[class*="fh"] >> text=/\\d{8,}/',
      'td:has-text("FH")',
      'div:has-text("FH") >> text=/FE?H\\d+/i'
    ];
    
    for (const selector of fhSelectors) {
      try {
        const element = page.locator(selector).first();
        if (await element.count() > 0) {
          const text = await element.textContent();
          const match = text?.match(/FE?H[\s-]?\d{8,}/i);
          if (match) {
            const fhNum = match[0].replace(/\s|-/g, '').toUpperCase();
            data.extractionAttempts.fhNumber.push({ strategy: `dom_${selector}`, value: fhNum });
            if (!data.fhNumber) data.fhNumber = fhNum;
          }
        }
      } catch (e) {
        // Continue to next strategy
      }
    }
    
    log.info(`   FH Number: ${data.fhNumber || 'NOT FOUND'} (${data.extractionAttempts.fhNumber.length} attempts)`);
    
    // === EXTRACT APPROVED AT DATE ===
    log.info('📅 Extracting Approved At Date...');
    
    const approvedPatterns = [
      /Approved\s+At\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Approved\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Approval\s+Date\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Date\s+Approved\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi
    ];
    
    for (const pattern of approvedPatterns) {
      const matches = [...normalizedText.matchAll(pattern)];
      if (matches && matches.length > 0) {
        const dateStr = matches[0][1];
        data.extractionAttempts.approvedAt.push({ strategy: 'text_pattern', value: dateStr });
        if (!data.approvedAt) data.approvedAt = dateStr;
      }
    }
    
    log.info(`   Approved At: ${data.approvedAt || 'NOT FOUND'} (${data.extractionAttempts.approvedAt.length} attempts)`);
    
    // === EXTRACT EXPIRATION DATE ===
    log.info('📅 Extracting Expiration Date...');
    
    const expirationPatterns = [
      /Expiration\s+Date\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Expiration\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Expires?\s+On\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Expires?\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi,
      /Valid\s+Until\s*:?\s*(\d{1,2}\/\d{1,2}\/\d{4})/gi
    ];
    
    for (const pattern of expirationPatterns) {
      const matches = [...normalizedText.matchAll(pattern)];
      if (matches && matches.length > 0) {
        const dateStr = matches[0][1];
        data.extractionAttempts.expirationDate.push({ strategy: 'text_pattern', value: dateStr });
        if (!data.expirationDate) data.expirationDate = dateStr;
      }
    }
    
    log.info(`   Expiration Date: ${data.expirationDate || 'NOT FOUND'} (${data.extractionAttempts.expirationDate.length} attempts)`);
    
    // === EXTRACT BUILDING ADDRESS ===
    log.info('🏠 Extracting Building Address...');
    
    const addressPatterns = [
      /Building\s+Address\s*:?\s*(\d+\s+[A-Za-z0-9\s.,#-]+(?:Dr|Drive|Rd|Road|St|Street|Ave|Avenue|Ln|Lane|Way|Cir|Circle|Blvd|Boulevard|Ct|Court|Pl|Place)\.?\s*[NSEWnsew]?)/gi,
      /Building\s+Address\s*:?\s*(\d+[^,\n]+)/gi,
      /Address\s*:?\s*(\d+\s+[A-Za-z0-9\s.,#-]+)/gi
    ];
    
    for (const pattern of addressPatterns) {
      const matches = [...normalizedText.matchAll(pattern)];
      if (matches && matches.length > 0) {
        const addressStr = matches[0][1].trim().replace(/\s+/g, ' ');
        data.extractionAttempts.buildingAddress.push({ strategy: 'text_pattern', value: addressStr });
        if (!data.buildingAddress) data.buildingAddress = addressStr;
      }
    }
    
    log.info(`   Building Address: ${data.buildingAddress || 'NOT FOUND'} (${data.extractionAttempts.buildingAddress.length} attempts)`);
    
    // === SUMMARY ===
    log.info('');
    log.info('📊 === EXTRACTION SUMMARY ===');
    log.info(`   ✓ FH Number: ${data.fhNumber || '❌ NOT FOUND'}`);
    log.info(`   ✓ Approved At: ${data.approvedAt || '❌ NOT FOUND'}`);
    log.info(`   ✓ Expiration Date: ${data.expirationDate || '❌ NOT FOUND'}`);
    log.info(`   ✓ Building Address: ${data.buildingAddress || '❌ NOT FOUND'}`);
    log.info('');
    
    return data;
    
  } catch (e) {
    log.error(`❌ Data extraction error: ${e.message}`);
    return data;
  }
}

// ==================== GOOGLE DRIVE UPLOAD ====================

async function uploadToGoogleDrive(buffer, fileName, mimeType = 'application/pdf') {
  const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;
  const clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  let privateKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;

  if (!folderId || !clientEmail || !privateKey) {
    log.warning('⚠️ Google Drive credentials not configured - skipping upload');
    return null;
  }

  try {
    privateKey = privateKey.replace(/\\n/g, '\n');

    const jwt = new google.auth.JWT({
      email: clientEmail,
      key: privateKey,
      scopes: ['https://www.googleapis.com/auth/drive.file'],
    });
    
    await jwt.authorize();

    const drive = google.drive({ version: 'v3', auth: jwt });
    const media = { mimeType, body: Readable.from(buffer) };

    const res = await drive.files.create({
      requestBody: { name: fileName, parents: [folderId] },
      media,
      fields: 'id, webViewLink',
    });
    
    log.info(`✅ Uploaded to Google Drive: ${res.data.webViewLink}`);
    return res.data;
  } catch (e) {
    log.error(`❌ Google Drive upload failed: ${e.message}`);
    return null;
  }
}

// ==================== LOGIN ====================

async function ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs }) {
  const emailSel  = 'input[type="email"], input[name="email"], input[autocomplete="username"]';
  const passSel   = 'input[type="password"], input[name="password"], input[autocomplete="current-password"]';
  const submitSel = 'button:has-text("Sign in"), button:has-text("Log in"), button[type="submit"]';

  log.info('🔐 Starting login process...');
  
  await page.goto(loginUrl, { waitUntil: 'domcontentloaded', timeout: 90_000 });
  await page.waitForLoadState('networkidle', { timeout: 60_000 }).catch(() => {});
  await page.waitForTimeout(3000);

  if (await page.locator(emailSel).count()) {
    await page.fill(emailSel, username);
    await sleep(jitter(200));
  }
  
  if (await page.locator(passSel).count()) {
    await page.fill(passSel, password);
    await sleep(jitter(200));
  }

  if (await page.locator(submitSel).count()) {
    await page.click(submitSel);
  } else {
    await page.press(passSel, 'Enter');
  }

  await page.waitForSelector('text=/New Evaluation/i', { timeout: 30_000, state: 'visible' });
  await page.waitForTimeout(3000);

  log.info('✅ Login successful!');
  await sleep(jitter(politeDelayMs));
}

// ==================== STORAGE ====================

async function loadProcessed() {
  const store = await Actor.openKeyValueStore();
  return (await store.getValue('processed_addresses')) || {};
}

async function saveProcessed(map) {
  const store = await Actor.openKeyValueStore();
  await store.setValue('processed_addresses', map);
}

// ==================== MAIN ACTOR ====================

async function run() {
  await Actor.init();

  const input = (await Actor.getInput()) || {};
  const {
    loginUrl = 'https://app.ibhs.org/fh',
    addresses: rawAddresses = [],
    address,
    maxAddressesPerRun = 100,
    politeDelayMs = 1000,
    username: usernameFromInput,
    password: passwordFromInput,
  } = input;

  // Parse addresses
  let addresses = [];
  if (address && typeof address === 'string') {
    addresses = [address.trim()];
  } else if (Array.isArray(rawAddresses)) {
    addresses = rawAddresses
      .map(a => typeof a === 'string' ? a.trim() : a.address?.trim())
      .filter(Boolean);
  }

  // Get credentials
  const username = usernameFromInput || process.env.IBHS_USERNAME;
  const password = passwordFromInput || process.env.IBHS_PASSWORD;

  if (!username || !password) {
    throw new Error('❌ Missing credentials! Set IBHS_USERNAME and IBHS_PASSWORD');
  }
  
  if (!addresses.length) {
    throw new Error('❌ No addresses provided!');
  }

  log.info(`📋 Processing ${addresses.length} address(es)`);

  const processed = await loadProcessed();

  // Launch browser
  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
  });

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119 Safari/537.36',
    acceptDownloads: true,
  });

  const page = await context.newPage();

  try {
    // Login
    await ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs });

    let handled = 0;
    
    for (const rawAddr of addresses) {
      if (handled >= maxAddressesPerRun) break;

      const addr = (rawAddr || '').trim();
      const key = normalizeAddress(addr);
      
      if (!key) continue;
      if (processed[key]) {
        log.info(`⏭️ Skipping (already processed): ${addr}`);
        continue;
      }

      log.info('');
      log.info(`${'='.repeat(60)}`);
      log.info(`🎯 PROCESSING: ${addr}`);
      log.info(`${'='.repeat(60)}`);
      log.info('');

      const result = {
        address: addr,
        searchAddress: key,
        timestamp: new Date().toISOString(),
        success: false,
        fhNumber: null,
        approvedAt: null,
        expirationDate: null,
        buildingAddress: null,
        screenshot: null,
        certificateFile: null,
        error: null
      };

      try {
        // Navigate to New Evaluation
        log.info('📋 Step 1: Clicking "New Evaluation"...');
        await page.waitForSelector('text=/^\\s*New Evaluation\\s*$/i', { 
          timeout: 30000, 
          state: 'visible' 
        });
        await page.getByText(/^\s*New Evaluation\s*$/i).first().click();
        await page.waitForTimeout(3000);
        await page.waitForLoadState('networkidle', { timeout: 60000 });

        // Click Redesignation
        log.info('🔄 Step 2: Clicking "Redesignation"...');
        await page.waitForSelector('text=/^\\s*Redesignation\\s*$/i', { 
          timeout: 30000,
          state: 'visible' 
        });
        await page.getByText(/^\s*Redesignation\s*$/i).first().click();
        await page.waitForTimeout(3000);
        await page.waitForLoadState('networkidle', { timeout: 60000 });

        // Search for address
        log.info(`🔍 Step 3: Searching for "${addr}"...`);
        await page.waitForTimeout(2000);
        
        const searchField = page.locator('input[placeholder*="Type to search" i]').nth(1);
        
        if (!(await searchField.count())) {
          throw new Error('Search field not found');
        }

        await searchField.click();
        await page.waitForTimeout(500);
        await searchField.press('Control+A');
        await searchField.press('Backspace');
        await page.waitForTimeout(500);
        
        // Type address character by character
        for (const char of addr) {
          await searchField.type(char, { delay: jitter(80, 60) });
        }
        
        await page.waitForTimeout(3000);

        // Select first result
        log.info('✅ Step 4: Selecting search result...');
        await page.keyboard.press('Enter');
        await page.waitForTimeout(3000);
        await page.waitForLoadState('networkidle', { timeout: 60000 });

        // Wait for certificate info to load
        log.info('⏳ Step 5: Waiting for certificate data to load...');
        await page.waitForTimeout(5000);

        // EXTRACT DATA
        const extractedData = await extractCertificateData(page);
        result.fhNumber = extractedData.fhNumber;
        result.approvedAt = extractedData.approvedAt;
        result.expirationDate = extractedData.expirationDate;
        result.buildingAddress = extractedData.buildingAddress;

        // CAPTURE SCREENSHOT (ALWAYS)
        log.info('📸 Step 6: Capturing final screenshot...');
        const screenshot = await captureAndSaveScreenshot(page, addr, 'certificate-info');
        result.screenshot = screenshot.url;
        result.screenshotKey = screenshot.key;

        // Try to download PDF
        log.info('📥 Step 7: Attempting PDF download...');
        const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
        let downloadButton = modalContainer.getByText(/^\s*Download\s*$/i).first();
        
        if (!(await downloadButton.count())) {
          downloadButton = page.getByText(/^\s*Download\s*$/i).first();
        }

        if (await downloadButton.count()) {
          const downloadPromise = page.waitForEvent('download', { timeout: 60_000 })
            .then(d => ({ kind: 'download', d }))
            .catch(() => null);

          await downloadButton.click();
          const signal = await downloadPromise;

          if (signal?.kind === 'download') {
            const stream = await signal.d.createReadStream();
            const buffer = stream ? await streamToBuffer(stream) : null;

            if (buffer && buffer.length > 0) {
              const fhNum = result.fhNumber || key;
              const expDate = result.expirationDate ? ` - Expires ${result.expirationDate}` : '';
              const fileName = sanitizeFileName(`${fhNum}${expDate}.pdf`);
              const kvKey = kvSafeKey(`${fhNum}-certificate.pdf`);

              // Save to KVS
              await Actor.setValue(kvKey, buffer, { contentType: 'application/pdf' });
              result.certificateFile = kvKey;
              result.certificateUrl = `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${kvKey}`;

              // Upload to Google Drive
              const driveFile = await uploadToGoogleDrive(buffer, fileName);
              if (driveFile) {
                result.googleDriveId = driveFile.id;
                result.googleDriveUrl = driveFile.webViewLink;
              }

              log.info(`✅ PDF downloaded: ${kvKey} (${buffer.length} bytes)`);
            }
          }
        } else {
          log.warning('⚠️ No download button found');
        }

        result.success = true;
        result.status = 'completed';

      } catch (error) {
        log.error(`❌ Error processing ${addr}: ${error.message}`);
        result.error = error.message;
        result.status = 'error';
        
        // Capture error screenshot
        const errorScreenshot = await captureAndSaveScreenshot(page, addr, 'error');
        result.screenshot = errorScreenshot.url;
        result.screenshotKey = errorScreenshot.key;
      }

      // Save result
      await Actor.pushData(result);

      processed[key] = {
        status: result.status,
        timestamp: result.timestamp,
        fhNumber: result.fhNumber,
        approvedAt: result.approvedAt,
        expirationDate: result.expirationDate
      };
      await saveProcessed(processed);

      log.info('');
      log.info('📊 RESULT SUMMARY:');
      log.info(`   FH Number: ${result.fhNumber || '❌ NOT FOUND'}`);
      log.info(`   Approved At: ${result.approvedAt || '❌ NOT FOUND'}`);
      log.info(`   Expiration Date: ${result.expirationDate || '❌ NOT FOUND'}`);
      log.info(`   Screenshot: ${result.screenshot ? '✅ SAVED' : '❌ FAILED'}`);
      log.info(`   Certificate: ${result.certificateFile ? '✅ DOWNLOADED' : '⚠️ NOT AVAILABLE'}`);
      log.info('');

      // Return to main page
      await page.goto(loginUrl, { waitUntil: 'networkidle' });
      await page.waitForTimeout(2000);

      handled++;
    }

    log.info(`✅ Run complete! Processed ${handled} addresses`);

  } finally {
    await browser.close().catch(() => {});
    await Actor.exit();
  }
}

Actor.main(run);