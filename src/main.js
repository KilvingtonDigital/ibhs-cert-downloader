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

// === NEW: SEARCH RESULTS SCREENSHOT FUNCTION ===
async function saveSearchResultScreenshot(page, address) {
  try {
    const timestamp = Date.now();
    const safeName = sanitizeFileName(address);
    
    // Full page screenshot of search results
    const fullPng = await page.screenshot({ fullPage: true });
    const fullKey = `search-results-${safeName}-${timestamp}.png`;
    await Actor.setValue(fullKey, fullPng, { contentType: 'image/png' });
    log.info(`📸 Saved search results screenshot: ${fullKey}`);
    
    // Try to screenshot just the results dropdown/modal if visible
    const dropdownSelectors = [
      '[role="listbox"]',
      '[class*="dropdown"]',
      '[class*="results"]',
      '[class*="options"]',
      '.dropdown-menu',
      '[class*="select__menu"]',
      '[class*="option"]'
    ];
    
    for (const selector of dropdownSelectors) {
      const dropdown = page.locator(selector).first();
      if (await dropdown.count() > 0 && await dropdown.isVisible()) {
        const dropdownPng = await dropdown.screenshot();
        const dropdownKey = `search-dropdown-${safeName}-${timestamp}.png`;
        await Actor.setValue(dropdownKey, dropdownPng, { contentType: 'image/png' });
        log.info(`📸 Saved dropdown screenshot: ${dropdownKey}`);
        return { fullKey, dropdownKey };
      }
    }
    
    return { fullKey, dropdownKey: null };
  } catch (e) {
    log.warning(`⚠️ Search screenshot save failed: ${e.message}`);
    return { fullKey: null, dropdownKey: null };
  }
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
    
    // Strategy 1: Look for text in modal or page
    const textPatterns = [
      page.locator('text=/FE?H\\d+/i').first(),
      page.locator('td, div, span, label').filter({ hasText: /FE?H\d+/i }).first(),
    ];

    for (const pattern of textPatterns) {
      if (await pattern.count()) {
        const text = await pattern.textContent();
        const match = text?.match(/FE?H\d+/i);
        if (match) {
          log.info(`Found FH/FEH number: ${match[0]}`);
          return match[0].toUpperCase();
        }
      }
    }

    // Strategy 2: Search input field for "Search by FID"
    const fidInput = page.locator('input[placeholder*="FID" i], input[value*="FH" i]');
    if (await fidInput.count()) {
      const value = await fidInput.inputValue();
      const match = value?.match(/FE?H\d+/i);
      if (match) {
        log.info(`Found FH/FEH number in input: ${match[0]}`);
        return match[0].toUpperCase();
      }
    }

    // Strategy 3: Extract from URL
    const url = page.url();
    const urlMatch = url.match(/FE?H\d+/i);
    if (urlMatch) {
      log.info(`Found FH/FEH number in URL: ${urlMatch[0]}`);
      return urlMatch[0].toUpperCase();
    }

    // Strategy 4: Full page text search
    const pageText = await page.textContent('body');
    const textMatch = pageText?.match(/FE?H\d+/i);
    if (textMatch) {
      log.info(`Found FH/FEH number in page text: ${textMatch[0]}`);
      return textMatch[0].toUpperCase();
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

    // Get all text content from the visible modal/page
    const pageText = await page.textContent('body');
    const normalizedText = pageText.replace(/\s+/g, ' ').trim();
    
    log.info('🔍 Searching for certificate fields in page content...');
    
    // Extract Approved At date
    const approvedPatterns = [
      /Approved\s+At[:\s]*(\d{2}\/\d{2}\/\d{4})/i,
      /Approved[:\s]*(\d{2}\/\d{2}\/\d{4})/i,
    ];
    
    for (const pattern of approvedPatterns) {
      const match = normalizedText.match(pattern);
      if (match) {
        info.approvedAt = match[1];
        log.info(`✅ Found Approved At: ${match[1]}`);
        break;
      }
    }
    
    // Extract Expiration Date
    const expirationPatterns = [
      /Expiration\s+Date[:\s]*(\d{2}\/\d{2}\/\d{4})/i,
      /Expiration[:\s]*(\d{2}\/\d{2}\/\d{4})/i,
    ];
    
    for (const pattern of expirationPatterns) {
      const match = normalizedText.match(pattern);
      if (match) {
        info.expirationDate = match[1];
        log.info(`✅ Found Expiration Date: ${match[1]}`);
        break;
      }
    }
    
    // Extract Building Address
    const addressPatterns = [
      /Building\s+Address[:\s]+(\d+\s+[A-Za-z\s]+(?:Dr|Drive|Rd|Road|St|Street|Ave|Avenue|Ln|Lane|Way|Cir|Circle|Blvd|Boulevard)\.?\s*[NSEWnsew]?)/i,
      /Building\s+Address[:\s]+(\d+\s+[A-Za-z\s]+)/i,
    ];
    
    for (const pattern of addressPatterns) {
      const match = normalizedText.match(pattern);
      if (match) {
        // Clean up the address (remove extra spaces)
        info.buildingAddress = match[1].trim().replace(/\s+/g, ' ');
        log.info(`✅ Found Building Address: ${info.buildingAddress}`);
        break;
      }
    }
    
    // Extract Building City
    const cityPatterns = [
      /Building\s+City[:\s]+([A-Za-z\s]+?)(?:\s+Building|\s+Mobile|\s+AL|\s+\d{5}|Hazard)/i,
    ];
    
    for (const pattern of cityPatterns) {
      const match = normalizedText.match(pattern);
      if (match) {
        info.buildingCity = match[1].trim();
        log.info(`✅ Found Building City: ${info.buildingCity}`);
        break;
      }
    }
    
    // Extract Building State
    const statePatterns = [
      /Building\s+State[:\s]+([A-Z]{2}\s*-\s*[A-Za-z\s]+)/i,
      /Building\s+State[:\s]+([A-Z]{2})/i,
    ];
    
    for (const pattern of statePatterns) {
      const match = normalizedText.match(pattern);
      if (match) {
        info.buildingState = match[1].trim();
        log.info(`✅ Found Building State: ${info.buildingState}`);
        break;
      }
    }
    
    // Extract Building Zip
    const zipPatterns = [
      /Building\s+Zip[:\s]+(\d{5})/i,
      /(\d{5})\s+Hazard\s+Type/i,
    ];
    
    for (const pattern of zipPatterns) {
      const match = normalizedText.match(pattern);
      if (match) {
        info.buildingZip = match[1];
        log.info(`✅ Found Building Zip: ${info.buildingZip}`);
        break;
      }
    }

    log.info('📊 Final Certificate Info Summary:');
    log.info(`   Approved At: ${info.approvedAt || 'NOT FOUND'}`);
    log.info(`   Expiration Date: ${info.expirationDate || 'NOT FOUND'}`);
    log.info(`   Building Address: ${info.buildingAddress || 'NOT FOUND'}`);
    log.info(`   Building City: ${info.buildingCity || 'NOT FOUND'}`);
    log.info(`   Building State: ${info.buildingState || 'NOT FOUND'}`);
    log.info(`   Building Zip: ${info.buildingZip || 'NOT FOUND'}`);
    
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

      let searchScreenshots = { fullKey: null, dropdownKey: null };
      let searchResultsData = {
        searchResultsCount: 0,
        selectedResult: null,
        matchFound: false
      };

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
        
        // === CAPTURE SEARCH RESULTS SCREENSHOTS ===
        log.info('📸 Capturing search results screenshots...');
        searchScreenshots = await saveSearchResultScreenshot(page, addr);
        
        if (debug) await debugPageState(page, 'ADDRESS_SEARCH_FILTERED_RESULTS');
        
        // Find and click matching address
        const addressParts = addr.trim().split(/\s+/);
        const streetNumber = addressParts[0];
        const streetName = addressParts.slice(1, 3).join(' ');
        
        const dropdownItems = page.locator('[role="option"], .dropdown-item, .result-item, [class*="option"], [class*="result"]');
        const itemCount = await dropdownItems.count();
        
        log.info(`Found ${itemCount} dropdown items`);
        searchResultsData.searchResultsCount = itemCount;
        
        let matchFound = false;
        let selectedItemText = null;
        
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
              selectedItemText = itemText.trim();
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
          // Get first item text before selecting
          if (itemCount > 0) {
            try {
              selectedItemText = await dropdownItems.first().textContent();
              selectedItemText = selectedItemText?.trim() || null;
            } catch (e) {
              log.debug('Could not read first item text');
            }
          }
          await page.keyboard.press('Enter');
          await page.waitForTimeout(2000);
          await page.waitForLoadState('networkidle', { timeout: 60000 });
        }
        
        searchResultsData.selectedResult = selectedItemText;
        searchResultsData.matchFound = matchFound;
        
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
            searchScreenshotFull: searchScreenshots.fullKey ? 
              `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${searchScreenshots.fullKey}` : null,
            searchScreenshotDropdown: searchScreenshots.dropdownKey ? 
              `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${searchScreenshots.dropdownKey}` : null,
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
            
            // Search results info
            searchScreenshotFull: searchScreenshots.fullKey ? 
              `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${searchScreenshots.fullKey}` : null,
            searchScreenshotDropdown: searchScreenshots.dropdownKey ? 
              `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${searchScreenshots.dropdownKey}` : null,
            searchResultsCount: searchResultsData.searchResultsCount,
            selectedResult: searchResultsData.selectedResult,
            matchFound: searchResultsData.matchFound,
            
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
          
          // Search results info
          searchScreenshotFull: searchScreenshots.fullKey ? 
            `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${searchScreenshots.fullKey}` : null,
          searchScreenshotDropdown: searchScreenshots.dropdownKey ? 
            `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${searchScreenshots.dropdownKey}` : null,
          searchResultsCount: searchResultsData.searchResultsCount,
          selectedResult: searchResultsData.selectedResult,
          matchFound: searchResultsData.matchFound,
          
          // Certificate info
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