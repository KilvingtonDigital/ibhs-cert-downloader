// src/main.js
import { Actor, log } from 'apify';
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';
import { Readable } from 'stream';
import { google } from 'googleapis';

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
    
    const png = await page.screenshot({ fullPage: true });
    const screenshotKey = `debug-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.png`;
    await Actor.setValue(screenshotKey, png, { contentType: 'image/png' });
    log.info(`📸 Screenshot saved as: ${screenshotKey}`);
    
    const html = await page.content();
    const htmlKey = `debug-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.html`;
    await Actor.setValue(htmlKey, html, { contentType: 'text/html' });
    log.info(`💾 HTML saved as: ${htmlKey}`);
    
  } catch (e) {
    log.error(`❌ Debug failed: ${e.message}`);
  }
  
  log.info(`🔍 === END DEBUG ${step} ===`);
}

async function debugModalState(page, step) {
  const timestamp = new Date().toISOString();
  log.info(`🔍 === DEBUG MODAL ${step} at ${timestamp} ===`);
  
  try {
    const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
    const modalExists = await modalContainer.count() > 0;
    
    log.info(`📦 Modal container exists: ${modalExists}`);
    
    if (modalExists) {
      const isVisible = await modalContainer.isVisible();
      log.info(`👁️ Modal visible: ${isVisible}`);
      
      if (isVisible) {
        const modalText = await modalContainer.textContent();
        log.info(`📝 Modal text length: ${modalText.length} characters`);
        
        const modalPng = await modalContainer.screenshot();
        const modalScreenshotKey = `debug-modal-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.png`;
        await Actor.setValue(modalScreenshotKey, modalPng, { contentType: 'image/png' });
        log.info(`📸 Modal screenshot saved as: ${modalScreenshotKey}`);
      }
    }
    
    const png = await page.screenshot({ fullPage: true });
    const screenshotKey = `debug-full-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.png`;
    await Actor.setValue(screenshotKey, png, { contentType: 'image/png' });
    log.info(`📸 Full screenshot saved as: ${screenshotKey}`);
    
  } catch (e) {
    log.error(`❌ Modal debug failed: ${e.message}`);
  }
  
  log.info(`🔍 === END DEBUG MODAL ${step} ===`);
}

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

async function loadProcessed() {
  const store = await Actor.openKeyValueStore();
  return (await store.getValue('processed_by_address')) || {};
}

async function saveProcessed(map) {
  const store = await Actor.openKeyValueStore();
  await store.setValue('processed_by_address', map);
}

async function uploadToGoogleDrive(buffer, fileName, mimeType = 'application/pdf') {
  const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;
  const clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  let privateKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;

  if (!folderId || !clientEmail || !privateKey) {
    log.warning('Google Drive env not fully configured; skipping Drive upload.');
    return null;
  }

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
  return res.data;
}

async function ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs, debug }) {
  const emailSel  = 'input[type="email"], input[name="email"], input[autocomplete="username"]';
  const passSel   = 'input[type="password"], input[name="password"], input[autocomplete="current-password"]';
  const submitSel = 'button:has-text("Sign in"), button:has-text("Log in"), button[type="submit"], [type="submit"]';

  log.info('🔑 Starting login process...');
  log.info(`📍 Navigating to: ${loginUrl}`);
  
  try {
    await page.goto(loginUrl, { waitUntil: 'domcontentloaded', timeout: 90_000 });
    log.info('✅ Page loaded (domcontentloaded)');
  } catch (e) {
    log.error(`❌ Failed to load page: ${e.message}`);
    throw e;
  }
  
  try {
    await page.waitForLoadState('networkidle', { timeout: 60_000 });
    log.info('✅ Network idle reached');
  } catch (e) {
    log.warning(`⚠️ Network idle timeout: ${e.message} - continuing anyway`);
  }
  
  // CRITICAL: Add extra wait for JavaScript to initialize
  log.info('⏳ Waiting for page JavaScript to initialize...');
  await page.waitForTimeout(5000);
  
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
  const appHint   = page.waitForSelector('text=/Certificates?|Search|New Evaluation/i', { timeout: 30_000 }).catch(() => null);
  await Promise.race([loginGone, appHint]);

  if (await page.locator(emailSel).count()) {
    if (await page.locator(submitSel).count()) await page.click(submitSel);
    await page.waitForLoadState('networkidle', { timeout: 30_000 });
  }

  // CRITICAL: Wait for the dashboard to fully render
  log.info('⏳ Waiting for dashboard to fully render...');
  await page.waitForTimeout(5000);
  
  // Wait for "New Evaluation" button to appear
  try {
    await page.waitForSelector('text=/New Evaluation/i', { 
      timeout: 30000,
      state: 'visible' 
    });
    log.info('✅ Dashboard loaded - "New Evaluation" button found');
  } catch (e) {
    log.warning(`⚠️ Could not find "New Evaluation" button: ${e.message}`);
  }

  if (debug) await debugPageState(page, 'POST_LOGIN');
  
  if (await page.locator(emailSel).count()) {
    throw new Error('Login did not complete (email field still visible). Check credentials or selector.');
  }

  log.info('✅ Login successful!');
  await sleep(jitter(politeDelayMs));
}

async function extractFHNumber(page) {
  try {
    log.info('🔢 Extracting FH/FEH number...');
    
    const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
    if (await modalContainer.count() > 0) {
      log.info('Searching for FH number in modal container...');
      
      const modalPatterns = [
        modalContainer.locator('text=/FE?H\\d+/i').first(),
        modalContainer.locator('td, div, span').filter({ hasText: /FE?H\d+/i }).first(),
      ];

      for (const pattern of modalPatterns) {
        if (await pattern.count()) {
          const text = await pattern.textContent();
          const match = text?.match(/FE?H\d+/i);
          if (match) {
            log.info(`Found FH/FEH number in modal: ${match[0]}`);
            return match[0].toUpperCase();
          }
        }
      }
    }
    
    const pagePatterns = [
      page.locator('text=/FE?H\\d+/i').first(),
      page.locator('td, div, span').filter({ hasText: /FE?H\d+/i }).first(),
    ];

    for (const pattern of pagePatterns) {
      if (await pattern.count()) {
        const text = await pattern.textContent();
        const match = text?.match(/FE?H\d+/i);
        if (match) {
          log.info(`Found FH/FEH number on page: ${match[0]}`);
          return match[0].toUpperCase();
        }
      }
    }

    const url = page.url();
    const urlMatch = url.match(/FE?H\d+/i);
    if (urlMatch) {
      log.info(`Found FH/FEH number in URL: ${urlMatch[0]}`);
      return urlMatch[0].toUpperCase();
    }

    log.warning('No FH/FEH number found anywhere on page');
    return null;
  } catch (e) {
    log.warning(`Error extracting FH/FEH number: ${e.message}`);
    return null;
  }
}

async function extractCertificateInfo(page) {
  try {
    const info = {
      approvedAt: null,
      expirationDate: null,
      buildingAddress: null,
      buildingCity: null,
      buildingState: null,
      buildingZip: null
    };

    log.info('📋 Extracting certificate info...');
    await page.waitForTimeout(2000);

    const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
    const modalExists = await modalContainer.count() > 0;
    
    if (modalExists) {
      log.info('Extracting from modal container...');
      const modalText = await modalContainer.textContent();
      const normalizedText = modalText.replace(/\s+/g, ' ').trim();
      
      const approvedMatch = normalizedText.match(/Approved\s+At[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
      if (approvedMatch) {
        info.approvedAt = approvedMatch[1];
        log.info(`Found Approved At: ${approvedMatch[1]}`);
      }
      
      const expirationMatch = normalizedText.match(/Expiration\s+Date[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
      if (expirationMatch) {
        info.expirationDate = expirationMatch[1];
        log.info(`Found Expiration Date: ${expirationMatch[1]}`);
      }
      
      const addressPatterns = [
        /Building\s+Address[:\s]+(\d+\s+[\w\s]+(?:Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Way|Cir|Circle)\s*[NSEWnsew]?)/i,
        /(\d+\s+[\w\s]+(?:Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Way|Cir|Circle)\s*[NSEWnsew]?)\s+Mobile/i,
      ];
      
      for (const pattern of addressPatterns) {
        const addressMatch = normalizedText.match(pattern);
        if (addressMatch) {
          info.buildingAddress = addressMatch[1].trim();
          log.info(`Found Building Address: ${addressMatch[1]}`);
          break;
        }
      }
    }

    return info;
  } catch (e) {
    log.warning(`Error extracting certificate info: ${e.message}`);
    return {
      approvedAt: null,
      expirationDate: null,
      buildingAddress: null,
      buildingCity: null,
      buildingState: null,
      buildingZip: null
    };
  }
}

async function run() {
  await Actor.init();

  const input = (await Actor.getInput()) || {};
  const {
    loginUrl = 'https://app.ibhs.org/fh',
    addresses: rawAddresses = [],
    address,
    maxAddressesPerRun = 100,
    politeDelayMs = 800,
    debug = false,
    returnStructuredData = true,
    username: usernameFromInput,
    password: passwordFromInput,
  } = input;

  let addresses = [];
  
  if (address && typeof address === 'string') {
    addresses = [address.trim()];
  } 
  else if (Array.isArray(rawAddresses)) {
    addresses = rawAddresses.map(a => typeof a === 'string' ? a.trim() : a.address?.trim()).filter(Boolean);
  } 
  else if (typeof rawAddresses === 'string') {
    addresses = rawAddresses.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  }

  const username = usernameFromInput || process.env.IBHS_USERNAME;
  const password = passwordFromInput || process.env.IBHS_PASSWORD;

  log.info(`🔐 Credentials check - Username: ${username ? 'SET' : 'MISSING'}, Password: ${password ? 'SET' : 'MISSING'}`);
  
  if (!username || !password) {
    log.error('❌ Missing credentials! Set IBHS_USERNAME and IBHS_PASSWORD in Actor secrets or input.');
    throw new Error('Missing credentials.');
  }
  if (!addresses.length) {
    log.error('❌ No addresses provided!');
    throw new Error('No addresses provided.');
  }

  log.info(`📋 Will process ${addresses.length} address(es): ${addresses.join(', ')}`);

  const processed = await loadProcessed();

  log.info('🌐 Launching browser...');
  const browser = await chromium.launch({
    headless: true,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage'],
  });

  const context = await browser.newContext({
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36',
    acceptDownloads: true,
  });

  const page = await context.newPage();
  log.info('✅ Browser launched successfully');

  try {
    log.info('🔓 Attempting login...');
    await ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs, debug });
    log.info('✅ Login completed successfully!');

    let handled = 0;
    for (const raw of addresses) {
      if (handled >= maxAddressesPerRun) break;

      const addr = (raw || '').trim();
      const key = normalizeAddress(addr);
      if (!key) continue;
      if (processed[key]) { 
        log.info(`Skip (already processed): ${addr}`); 
        continue; 
      }

      log.info(`🎯 Processing address: ${addr}`);

      log.info('📋 Clicking "New Evaluation"...');
      
      // CRITICAL: Wait for button to be ready
      try {
        await page.waitForSelector('text=/^\\s*New Evaluation\\s*$/i', { 
          timeout: 30000,
          state: 'visible' 
        });
        log.info('✅ "New Evaluation" button is visible');
      } catch (e) {
        log.error(`❌ "New Evaluation" button not visible: ${e.message}`);
        await debugPageState(page, 'NO_NEW_EVALUATION_BUTTON');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'navigation_error',
            error: 'New Evaluation button not visible',
            timestamp: new Date().toISOString(),
            success: false,
            processedAt: new Date().toISOString()
          });
        }
        
        processed[key] = { status: 'navigation_error' };
        await saveProcessed(processed);
        handled++;
        continue;
      }
      
      const newEvalButton = page.getByText(/^\s*New Evaluation\s*$/i).first();
      await newEvalButton.click();
      await page.waitForTimeout(3000); // Give extra time for transition
      await page.waitForLoadState('networkidle', { timeout: 60000 });

      if (debug) await debugPageState(page, 'AFTER_NEW_EVALUATION_CLICK');

      log.info('🔄 Clicking "Redesignation"...');
      
      // Wait for redesignation button
      try {
        await page.waitForSelector('text=/^\\s*Redesignation\\s*$/i', { 
          timeout: 30000,
          state: 'visible' 
        });
      } catch (e) {
        log.error(`❌ Redesignation button not visible: ${e.message}`);
        await debugPageState(page, 'NO_REDESIGNATION_BUTTON');
      }
      
      const redesignationButton = page.getByText(/^\s*Redesignation\s*$/i).first();
      await redesignationButton.click();
      await page.waitForTimeout(3000);
      await page.waitForLoadState('networkidle', { timeout: 60000 });

      if (debug) await debugPageState(page, 'AFTER_REDESIGNATION_CLICK');

      log.info(`🔍 Searching for address: ${addr}`);
      
      // Wait for modal to fully load
      await page.waitForTimeout(3000);
      
      // Find the address search field
      let searchField = page.locator('input[placeholder*="Type to search" i]').nth(1);
      
      if (!(await searchField.count())) {
        log.error('❌ Search field not found');
        await debugPageState(page, 'NO_SEARCH_FIELD_REDESIGNATION');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'no_search_field',
            error: 'Search field not found in redesignation flow',
            timestamp: new Date().toISOString(),
            success: false,
            processedAt: new Date().toISOString()
          });
        }
        
        processed[key] = { status: 'no_search_field' };
        await saveProcessed(processed);
        handled++;
        continue;
      }

      try {
        log.info(`📝 Typing address: ${addr}`);
        await searchField.click();
        await page.waitForTimeout(500);
        
        await searchField.press('Control+A');
        await page.waitForTimeout(100);
        await searchField.press('Backspace');
        await page.waitForTimeout(500);
        
        // Type character by character
        for (const char of addr) {
          await searchField.type(char, { delay: jitter(100, 80) });
        }
        
        log.info('⏳ Waiting for search results...');
        await page.waitForTimeout(3000);
        
        if (debug) await debugPageState(page, 'ADDRESS_SEARCH_FILTERED_RESULTS');
        
        // Find and click matching address
        const addressParts = addr.trim().split(/\s+/);
        const streetNumber = addressParts[0];
        const streetName = addressParts.slice(1, 3).join(' ');
        
        const dropdownItems = page.locator('[role="option"], .dropdown-item, .result-item, [class*="option"], [class*="result"]');
        const itemCount = await dropdownItems.count();
        
        log.info(`Found ${itemCount} dropdown items`);
        
        let matchFound = false;
        
        for (let i = 0; i < itemCount; i++) {
          const item = dropdownItems.nth(i);
          try {
            const itemText = await item.textContent();
            const itemTextLower = (itemText || '').toLowerCase();
            const streetNumberLower = streetNumber.toLowerCase();
            const streetNameLower = streetName.toLowerCase();
            
            if (itemTextLower.includes(streetNumberLower) && 
                itemTextLower.includes(streetNameLower)) {
              log.info(`✅ Found matching address at position ${i}!`);
              await item.click();
              matchFound = true;
              await page.waitForTimeout(2000);
              await page.waitForLoadState('networkidle', { timeout: 60000 });
              break;
            }
          } catch (e) {
            log.debug(`Could not read item ${i}: ${e.message}`);
          }
        }
        
        if (!matchFound) {
          log.info('Pressing Enter to select first result...');
          await page.keyboard.press('Enter');
          await page.waitForTimeout(2000);
          await page.waitForLoadState('networkidle', { timeout: 60000 });
        }
        
        // CRITICAL: Extra wait for certificate info to load
        log.info('⏳ Waiting for certificate information to load...');
        await page.waitForTimeout(5000);
        
        if (debug) await debugPageState(page, 'AFTER_ADDRESS_SELECT');
        
      } catch (searchError) {
        log.error(`❌ Search failed: ${searchError.message}`);
        await debugPageState(page, 'SEARCH_ERROR');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'search_failed',
            error: `Search failed: ${searchError.message}`,
            timestamp: new Date().toISOString(),
            success: false,
            processedAt: new Date().toISOString()
          });
        }
        
        processed[key] = { status: 'search_failed', error: searchError.message };
        await saveProcessed(processed);
        handled++;
        continue;
      }

      // Extract certificate info
      log.info('📅 Extracting certificate information...');
      await page.waitForTimeout(2000);
      
      if (debug) await debugModalState(page, 'CERTIFICATE_INFO_EXTRACTION');
      
      const fhNumber = await extractFHNumber(page);
      const certInfo = await extractCertificateInfo(page);
      
      log.info('📋 Certificate Information:');
      log.info(`   FH Number: ${fhNumber || 'NOT FOUND'}`);
      log.info(`   Building Address: ${certInfo.buildingAddress || 'NOT FOUND'}`);
      log.info(`   Approved At: ${certInfo.approvedAt || 'NOT FOUND'}`);
      log.info(`   Expiration Date: ${certInfo.expirationDate || 'NOT FOUND'}`);

      // Look for download button
      log.info('🔍 Looking for download button...');
      await page.waitForTimeout(1000);

      const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
      let downloadButton = modalContainer.getByText(/^\s*Download\s*$/i).first();
      
      if (!(await downloadButton.count())) {
        downloadButton = page.getByText(/^\s*Download\s*$/i).first();
      }
      
      const hasDownload = await downloadButton.count();

      if (!hasDownload) {
        log.warning('No Download button found');
        
        if (debug) await debugModalState(page, 'NO_DOWNLOAD_BUTTON');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'no_certificate',
            error: 'No download button found',
            fhNumber: fhNumber || null,
            buildingAddress: certInfo.buildingAddress || null,
            approvedAt: certInfo.approvedAt || null,
            expirationDate: certInfo.expirationDate || null,
            timestamp: new Date().toISOString(),
            success: false,
            processedAt: new Date().toISOString()
          });
        }
        
        processed[key] = { 
          status: 'no_certificate',
          fhNumber: fhNumber || null,
          buildingAddress: certInfo.buildingAddress || null,
          approvedAt: certInfo.approvedAt || null,
          expirationDate: certInfo.expirationDate || null,
        };
        await saveProcessed(processed);
        handled++;
        
        await page.goto(loginUrl, { waitUntil: 'networkidle' });
        continue;
      }

      log.info('✅ Found Download button');

      if (debug) await debugModalState(page, 'PRE_DOWNLOAD');

      const downloadPromise = page.waitForEvent('download', { timeout: 120_000 }).then(d => ({ kind: 'download', d })).catch(() => null);
      const popupPromise = page.waitForEvent('popup', { timeout: 120_000 }).then(p => ({ kind: 'popup', p })).catch(() => null);
      const responsePromise = page.waitForResponse(
        resp => (resp.headers()['content-type'] || '').toLowerCase().includes('application/pdf'),
        { timeout: 120_000 }
      ).then(r => ({ kind: 'response', r })).catch(() => null);

      await downloadButton.click({ delay: jitter(80, 160) });

      let signal = await Promise.race([downloadPromise, popupPromise, responsePromise]);
      if (!signal) {
        for (let waited = 0; waited < 25000 && !signal; waited += 500) {
          await page.waitForTimeout(500);
          signal = await Promise.race([downloadPromise, popupPromise, responsePromise]);
        }
      }

      let buffer = null;

      if (signal?.kind === 'download') {
        const dl = signal.d;
        log.info('Download event detected');
        try {
          const stream = await dl.createReadStream();
          if (stream) buffer = await streamToBuffer(stream);
          else {
            const filePath = await dl.path();
            buffer = await fs.readFile(filePath);
          }
        } catch (e) {
          log.warning(`Download read failed: ${e.message}`);
        }
      } else if (signal?.kind === 'response') {
        log.info('PDF response detected');
        buffer = await signal.r.body();
      } else if (signal?.kind === 'popup') {
        log.info('Popup detected');
        const p = signal.p;
        await p.waitForLoadState('domcontentloaded', { timeout: 60_000 }).catch(() => {});
        
        const pdfResp = await p.waitForResponse(
          resp => (resp.headers()['content-type'] || '').toLowerCase().includes('application/pdf'),
          { timeout: 60_000 }
        ).catch(() => null);

        if (pdfResp) {
          buffer = await pdfResp.body();
        }
      }

      const basePretty = `${fhNumber || key}${certInfo.expirationDate ? ` - Expires ${certInfo.expirationDate}` : ''}`.replace(/\s+/g, ' ');
      const prettyName = sanitizeFileName(`${basePretty}.pdf`);
      const kvName = kvSafeKey(`${fhNumber || key}-certificate.pdf`);

      if (!buffer || buffer.length === 0) {
        log.warning(`PDF buffer empty for ${key}`);
      } else {
        await Actor.setValue(kvName, buffer, { contentType: 'application/pdf' });
        log.info(`Saved to KVS: ${kvName} (${buffer.length} bytes)`);
      }

      let driveFileId = null;
      let driveWebViewLink = null;
      try {
        if (buffer && buffer.length > 0) {
          const uploaded = await uploadToGoogleDrive(buffer, prettyName);
          if (uploaded?.id) {
            driveFileId = uploaded.id;
            driveWebViewLink = uploaded.webViewLink;
            log.info(`Uploaded to Google Drive: ${uploaded.webViewLink || uploaded.id}`);
          }
        }
      } catch (e) {
        log.warning(`Google Drive upload error: ${e.message}`);
      }

      const downloadStatus = buffer && buffer.length > 0 ? 'downloaded' : 'empty_pdf';
      
      if (returnStructuredData) {
        await Actor.pushData({
          address: addr,
          searchAddress: key,
          status: downloadStatus,
          success: true,
          fhNumber: fhNumber || null,
          buildingAddress: certInfo.buildingAddress || null,
          approvedAt: certInfo.approvedAt || null,
          expirationDate: certInfo.expirationDate || null,
          certificateFile: kvName,
          fileName: prettyName,
          fileSize: buffer ? buffer.length : 0,
          googleDriveFileId: driveFileId,
          googleDriveLink: driveWebViewLink,
          apifyDownloadUrl: driveFileId ? null : `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${kvName}`,
          timestamp: new Date().toISOString(),
          downloadedAt: new Date().toISOString(),
          processedAt: new Date().toISOString()
        });
      }

      processed[key] = { 
        status: downloadStatus, 
        file: kvName, 
        when: new Date().toISOString(),
        fhNumber: fhNumber || null,
        buildingAddress: certInfo.buildingAddress || null,
        fileSize: buffer ? buffer.length : 0,
        approvedAt: certInfo.approvedAt || null,
        expirationDate: certInfo.expirationDate || null,
      };
      await saveProcessed(processed);

      log.info('🔙 Returning to main page...');
      await page.goto(loginUrl, { waitUntil: 'networkidle' });
      await page.waitForTimeout(2000);

      handled++;
    }

    log.info(`✅ Run complete. Addresses handled: ${handled}`);
    
  } finally {
    await browser.close().catch(() => {});
    await Actor.exit();
  }
}

Actor.main(run);