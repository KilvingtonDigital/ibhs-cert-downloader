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
    
    const inputs = await page.locator('input').all();
    for (let i = 0; i < Math.min(inputs.length, 5); i++) {
      const input = inputs[i];
      try {
        const attrs = await input.evaluate(el => ({
          type: el.type || 'text',
          placeholder: el.placeholder || '',
          name: el.name || '',
          id: el.id || '',
          visible: el.offsetParent !== null
        }));
        log.info(`  Input ${i}: ${JSON.stringify(attrs)}`);
      } catch (e) {
        log.info(`  Input ${i}: Could not read attributes`);
      }
    }
    
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

/**
 * Enhanced debug function to capture modal state
 */
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
        log.info(`📝 Modal text preview: ${modalText.substring(0, 300)}...`);
        
        // Save modal-specific screenshot
        const modalPng = await modalContainer.screenshot();
        const modalScreenshotKey = `debug-modal-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.png`;
        await Actor.setValue(modalScreenshotKey, modalPng, { contentType: 'image/png' });
        log.info(`📸 Modal screenshot saved as: ${modalScreenshotKey}`);
        
        // Save modal HTML
        const modalHtml = await modalContainer.innerHTML();
        const modalHtmlKey = `debug-modal-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.html`;
        await Actor.setValue(modalHtmlKey, modalHtml, { contentType: 'text/html' });
        log.info(`💾 Modal HTML saved as: ${modalHtmlKey}`);
      }
    }
    
    // Also capture full page for context
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

function safeKey(prefix, label, ext) {
  const clean = (label || '')
    .toLowerCase()
    .replace(/[^a-z0-9!._'()\-]+/g, '-')
    .slice(0, 80);
  return `${prefix}-${Date.now()}-${clean}.${ext}`;
}

async function snapshot(page, label) {
  try {
    const png = await page.screenshot({ fullPage: true });
    await Actor.setValue(safeKey('shot', label, 'png'), png, { contentType: 'image/png' });
    const html = await page.content();
    await Actor.setValue(safeKey('html', label, 'html'), html, { contentType: 'text/html' });
  } catch (e) {
    log.warning(`Snapshot failed: ${e.message}`);
  }
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

  if (debug) await debugPageState(page, 'POST_LOGIN');
  
  if (await page.locator(emailSel).count()) {
    throw new Error('Login did not complete (email field still visible). Check credentials or selector.');
  }

  log.info('✅ Login successful!');
  await sleep(jitter(politeDelayMs));
}

/**
 * Extract FH/FEH number from page (try modal first, then entire page)
 */
async function extractFHNumber(page) {
  try {
    log.info('🔢 Extracting FH/FEH number...');
    
    // Strategy 1: Try modal container first
    const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
    if (await modalContainer.count() > 0) {
      log.info('Searching for FH number in modal container...');
      
      const modalPatterns = [
        modalContainer.locator('text=/FE?H\\d+/i').first(),
        modalContainer.locator('td, div, span').filter({ hasText: /FE?H\d+/i }).first(),
        modalContainer.locator('[class*="fortified"], [id*="fortified"], text=/FORTIFIED ID/i').locator('..').locator('text=/FE?H\\d+/i').first(),
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
    
    // Strategy 2: Search entire page if modal doesn't work
    log.info('Searching for FH number in entire page...');
    const pagePatterns = [
      page.locator('text=/FE?H\\d+/i').first(),
      page.locator('td, div, span').filter({ hasText: /FE?H\d+/i }).first(),
      page.locator('[class*="fortified"], [id*="fortified"], text=/FORTIFIED ID/i').locator('..').locator('text=/FE?H\\d+/i').first(),
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

    // Strategy 3: Extract from URL
    const url = page.url();
    const urlMatch = url.match(/FE?H\d+/i);
    if (urlMatch) {
      log.info(`Found FH/FEH number in URL: ${urlMatch[0]}`);
      return urlMatch[0].toUpperCase();
    }

    // Strategy 4: Text search in entire page content
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

/**
 * Extract certificate dates and building address (try modal first, then entire page)
 */
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
    await page.waitForTimeout(1000);

    // Strategy 1: Try modal container first
    const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
    const modalExists = await modalContainer.count() > 0;
    
    if (modalExists) {
      log.info('Trying to extract from modal container...');
      
      try {
        const modalElements = await modalContainer.locator('text=/Approved At|Expiration Date|Building Address|Building City|Building State|Building Zip/i').all();
        
        for (const element of modalElements) {
          try {
            const text = await element.textContent();
            const parent = element.locator('..');
            const parentText = await parent.textContent();
            const container = element.locator('../..');
            const containerText = await container.textContent();
            const searchText = `${parentText} ${containerText}`;
            
            if (/Approved At/i.test(text)) {
              const dateMatch = searchText.match(/(\d{2}\/\d{2}\/\d{4})/);
              if (dateMatch && !info.approvedAt) {
                info.approvedAt = dateMatch[1];
                log.info(`Found Approved At in modal: ${dateMatch[1]}`);
              }
            }
            
            if (/Expiration Date/i.test(text)) {
              const dateMatch = searchText.match(/(\d{2}\/\d{2}\/\d{4})/);
              if (dateMatch && !info.expirationDate) {
                info.expirationDate = dateMatch[1];
                log.info(`Found Expiration Date in modal: ${dateMatch[1]}`);
              }
            }
            
            if (/Building Address/i.test(text) && !/Building Address 2/i.test(text)) {
              const addressMatch = searchText.match(/(\d+\s+[\w\s]+?)(?:\s+Building|\s+Mobile|\s+AL|\s+\d{5}|$)/i);
              if (addressMatch && !info.buildingAddress) {
                info.buildingAddress = addressMatch[1].trim();
                log.info(`Found Building Address in modal: ${addressMatch[1]}`);
              }
            }
          } catch (e) {
            log.debug(`Could not process modal element: ${e.message}`);
          }
        }
        
        // Try modal text extraction if DOM approach didn't work
        if (!info.approvedAt || !info.expirationDate || !info.buildingAddress) {
          const modalText = await modalContainer.textContent();
          const normalizedText = modalText.replace(/\s+/g, ' ').trim();
          
          if (!info.approvedAt) {
            const approvedMatch = normalizedText.match(/Approved\s+At[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
            if (approvedMatch) {
              info.approvedAt = approvedMatch[1];
              log.info(`Found Approved At in modal text: ${approvedMatch[1]}`);
            }
          }
          
          if (!info.expirationDate) {
            const expirationMatch = normalizedText.match(/Expiration\s+Date[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
            if (expirationMatch) {
              info.expirationDate = expirationMatch[1];
              log.info(`Found Expiration Date in modal text: ${expirationMatch[1]}`);
            }
          }
          
          if (!info.buildingAddress) {
            const addressPatterns = [
              /Building\s+Address[:\s]+(\d+\s+[\w\s]+(?:Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Way|Cir|Circle)\s*[NSEWnsew]?)/i,
              /(\d+\s+[\w\s]+(?:Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Way|Cir|Circle)\s*[NSEWnsew]?)\s+Mobile/i,
              /(\d+\s+[\w\s]+)\s+Mobile\s+AL/i
            ];
            
            for (const pattern of addressPatterns) {
              const addressMatch = normalizedText.match(pattern);
              if (addressMatch) {
                info.buildingAddress = addressMatch[1].trim();
                log.info(`Found Building Address in modal text: ${addressMatch[1]}`);
                break;
              }
            }
          }
        }
      } catch (e) {
        log.debug(`Modal extraction failed: ${e.message}`);
      }
    }

    // Strategy 2: Try entire page if modal extraction didn't work
    if (!info.approvedAt || !info.expirationDate || !info.buildingAddress) {
      log.info('Trying to extract from entire page...');
      
      try {
        const pageElements = await page.locator('text=/Approved At|Expiration Date|Building Address|Building City|Building State|Building Zip/i').all();
        
        for (const element of pageElements) {
          try {
            const text = await element.textContent();
            const parent = element.locator('..');
            const parentText = await parent.textContent();
            const container = element.locator('../..');
            const containerText = await container.textContent();
            const searchText = `${parentText} ${containerText}`;
            
            if (/Approved At/i.test(text) && !info.approvedAt) {
              const dateMatch = searchText.match(/(\d{2}\/\d{2}\/\d{4})/);
              if (dateMatch) {
                info.approvedAt = dateMatch[1];
                log.info(`Found Approved At on page: ${dateMatch[1]}`);
              }
            }
            
            if (/Expiration Date/i.test(text) && !info.expirationDate) {
              const dateMatch = searchText.match(/(\d{2}\/\d{2}\/\d{4})/);
              if (dateMatch) {
                info.expirationDate = dateMatch[1];
                log.info(`Found Expiration Date on page: ${dateMatch[1]}`);
              }
            }
            
            if (/Building Address/i.test(text) && !/Building Address 2/i.test(text) && !info.buildingAddress) {
              const addressMatch = searchText.match(/(\d+\s+[\w\s]+?)(?:\s+Building|\s+Mobile|\s+AL|\s+\d{5}|$)/i);
              if (addressMatch) {
                info.buildingAddress = addressMatch[1].trim();
                log.info(`Found Building Address on page: ${addressMatch[1]}`);
              }
            }
          } catch (e) {
            log.debug(`Could not process page element: ${e.message}`);
          }
        }
      } catch (e) {
        log.debug(`Page DOM extraction failed: ${e.message}`);
      }
    }

    // Strategy 3: Full page text extraction as final fallback
    if (!info.approvedAt || !info.expirationDate || !info.buildingAddress) {
      log.info('Trying full page text extraction...');
      
      try {
        const pageText = await page.textContent('body');
        const normalizedText = pageText.replace(/\s+/g, ' ').trim();
        
        if (!info.approvedAt) {
          const approvedMatch = normalizedText.match(/Approved\s+At[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
          if (approvedMatch) {
            info.approvedAt = approvedMatch[1];
            log.info(`Found Approved At in page text: ${approvedMatch[1]}`);
          }
        }
        
        if (!info.expirationDate) {
          const expirationMatch = normalizedText.match(/Expiration\s+Date[:\s]*(\d{2}\/\d{2}\/\d{4})/i);
          if (expirationMatch) {
            info.expirationDate = expirationMatch[1];
            log.info(`Found Expiration Date in page text: ${expirationMatch[1]}`);
          }
        }
        
        if (!info.buildingAddress) {
          const addressPatterns = [
            /Building\s+Address[:\s]+(\d+\s+[\w\s]+(?:Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Way|Cir|Circle)\s*[NSEWnsew]?)/i,
            /(\d+\s+[\w\s]+(?:Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Way|Cir|Circle)\s*[NSEWnsew]?)\s+Mobile/i,
            /(\d+\s+[\w\s]+)\s+Mobile\s+AL/i
          ];
          
          for (const pattern of addressPatterns) {
            const addressMatch = normalizedText.match(pattern);
            if (addressMatch) {
              info.buildingAddress = addressMatch[1].trim();
              log.info(`Found Building Address in page text: ${addressMatch[1]}`);
              break;
            }
          }
        }
      } catch (e) {
        log.warning(`Page text extraction failed: ${e.message}`);
      }
    }

    log.info('Final Certificate Info Summary:');
    log.info(`   Approved At: ${info.approvedAt || 'NOT FOUND'}`);
    log.info(`   Expiration Date: ${info.expirationDate || 'NOT FOUND'}`);
    log.info(`   Building Address: ${info.buildingAddress || 'NOT FOUND'}`);
    log.info(`   Building City: ${info.buildingCity || 'NOT FOUND'}`);
    log.info(`   Building State: ${info.buildingState || 'NOT FOUND'}`);
    
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
  
  // Handle single address (n8n will often send one at a time)
  if (address && typeof address === 'string') {
    addresses = [address.trim()];
  } 
  // Handle array of addresses (batch processing)
  else if (Array.isArray(rawAddresses)) {
    addresses = rawAddresses.map(a => typeof a === 'string' ? a.trim() : a.address?.trim()).filter(Boolean);
  } 
  // Handle newline-separated addresses
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
    headless: !debug,
    slowMo: debug ? 250 : 0,
    args: ['--disable-blink-features=AutomationControlled', '--no-sandbox'],
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

      // === NEW FLOW: Click "New Evaluation" ===
      log.info('📋 Clicking "New Evaluation"...');
      const newEvalButton = page.getByText(/^\s*New Evaluation\s*$/i).first();
      
      if (!(await newEvalButton.count())) {
        log.error('❌ "New Evaluation" button not found');
        await debugPageState(page, 'NO_NEW_EVALUATION_BUTTON');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'navigation_error',
            error: 'New Evaluation button not found',
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

      await newEvalButton.click();
      await sleep(jitter(politeDelayMs));
      await page.waitForLoadState('networkidle');

      if (debug) await debugPageState(page, 'AFTER_NEW_EVALUATION_CLICK');

      // === Click "Redesignation" ===
      log.info('🔄 Clicking "Redesignation"...');
      const redesignationButton = page.getByText(/^\s*Redesignation\s*$/i).first();
      
      if (!(await redesignationButton.count())) {
        log.error('❌ "Redesignation" button not found');
        await debugPageState(page, 'NO_REDESIGNATION_BUTTON');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'navigation_error',
            error: 'Redesignation button not found',
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

      await redesignationButton.click();
      await sleep(jitter(politeDelayMs));
      await page.waitForLoadState('networkidle');

      if (debug) await debugPageState(page, 'AFTER_REDESIGNATION_CLICK');

      // === Search for address ===
      log.info(`🔍 Searching for address: ${addr}`);
      
      // Wait for the modal to fully load
      await page.waitForTimeout(2000);
      
      // Look specifically for "Search by Address" section
      const addressSearchLabel = page.locator('text=/Search by Address/i');
      
      if (!(await addressSearchLabel.count())) {
        log.error('❌ "Search by Address" label not found');
        await debugPageState(page, 'NO_ADDRESS_SEARCH_LABEL');
      }
      
      // Get the search field that comes after "Search by Address"
      let searchField = null;
      
      const strategies = [
        // Strategy 1: Find input after "Search by Address" text
        () => page.locator('text=/Search by Address/i').locator('..').locator('input[placeholder*="Type to search" i]'),
        // Strategy 2: Find the second "Type to search" input in the modal
        () => page.locator('input[placeholder*="Type to search" i]').nth(1),
        // Strategy 3: Find input with specific structure
        () => page.locator('input[placeholder*="Type to search" i]').filter({ hasText: '' }).nth(1),
      ];
      
      for (let i = 0; i < strategies.length; i++) {
        try {
          const element = strategies[i]();
          if (await element.count() > 0 && await element.isVisible()) {
            searchField = element;
            log.info(`✅ Found address search field using strategy ${i + 1}`);
            break;
          }
        } catch (e) {
          log.debug(`Strategy ${i + 1} failed: ${e.message}`);
        }
      }
      
      if (!searchField) {
        searchField = page.locator('input[placeholder*="Type to search" i]').nth(1);
      }
      
      if (!(await searchField.count())) {
        log.error('❌ Search field not found in redesignation flow');
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
        log.info(`📝 Typing address into search field: ${addr}`);
        await searchField.click();
        await page.waitForTimeout(500);
        
        // Clear the field first
        await searchField.press('Control+A');
        await page.waitForTimeout(100);
        await searchField.press('Backspace');
        await page.waitForTimeout(300);
        
        // Type the address CHARACTER BY CHARACTER to trigger filtering
        log.info('⌨️ Typing character by character...');
        for (const char of addr) {
          await searchField.type(char, { delay: jitter(100, 80) });
        }
        
        // Wait for dropdown to filter and populate
        log.info('⏳ Waiting for filtered search results...');
        await page.waitForTimeout(2000);
        
        if (debug) await debugPageState(page, 'ADDRESS_SEARCH_FILTERED_RESULTS');
        
        // Look for dropdown results
        const addressParts = addr.trim().split(/\s+/);
        const streetNumber = addressParts[0];
        const streetName = addressParts.slice(1, 3).join(' ');
        
        log.info(`Looking for address with: ${streetNumber} ${streetName}`);
        
        // Try to find exact match in filtered dropdown
        const dropdownItems = page.locator('[role="option"], .dropdown-item, .result-item, [class*="option"], [class*="result"]');
        const itemCount = await dropdownItems.count();
        
        log.info(`Found ${itemCount} filtered dropdown items`);
        
        let matchFound = false;
        
        // Iterate through dropdown items to find exact match
        for (let i = 0; i < itemCount; i++) {
          const item = dropdownItems.nth(i);
          try {
            const itemText = await item.textContent();
            const preview = itemText?.substring(0, 80) || '';
            log.info(`  Item ${i}: ${preview}`);
            
            const itemTextLower = (itemText || '').toLowerCase();
            const streetNumberLower = streetNumber.toLowerCase();
            const streetNameLower = streetName.toLowerCase();
            
            // Check if this item contains our street number AND street name
            if (itemTextLower.includes(streetNumberLower) && 
                itemTextLower.includes(streetNameLower)) {
              log.info(`✅ Found matching address at position ${i}!`);
              await item.click();
              matchFound = true;
              await sleep(jitter(politeDelayMs));
              await page.waitForLoadState('networkidle');
              break;
            }
          } catch (e) {
            log.debug(`Could not read item ${i}: ${e.message}`);
          }
        }
        
        if (!matchFound) {
          log.warning('⚠️ No exact match found in filtered dropdown');
          
          // Try pressing Enter to select the first filtered result
          log.info('Pressing Enter to select first result...');
          await page.keyboard.press('Enter');
          await sleep(jitter(politeDelayMs));
          await page.waitForLoadState('networkidle');
        }
        
        if (debug) await debugPageState(page, 'AFTER_ADDRESS_SELECT');
        
        // ALWAYS take a comprehensive screenshot after address selection to see what's displayed
        log.info('📸 Taking comprehensive screenshot after address selection...');
        try {
          // Wait a bit for any animations or loading
          await page.waitForTimeout(3000);
          
          // Take full page screenshot
          const fullPagePng = await page.screenshot({ fullPage: true });
          const fullPageKey = `search-result-full-${key}-${Date.now()}.png`;
          await Actor.setValue(fullPageKey, fullPagePng, { contentType: 'image/png' });
          log.info(`📸 Full page screenshot saved: ${fullPageKey}`);
          
          // Take viewport screenshot
          const viewportPng = await page.screenshot({ fullPage: false });
          const viewportKey = `search-result-viewport-${key}-${Date.now()}.png`;
          await Actor.setValue(viewportKey, viewportPng, { contentType: 'image/png' });
          log.info(`📸 Viewport screenshot saved: ${viewportKey}`);
          
          // Save the current page HTML
          const pageHtml = await page.content();
          const htmlKey = `search-result-html-${key}-${Date.now()}.html`;
          await Actor.setValue(htmlKey, pageHtml, { contentType: 'text/html' });
          log.info(`💾 Page HTML saved: ${htmlKey}`);
          
          // Check for any modals, overlays, or popups
          const modalSelectors = [
            '[class*="modal"]',
            '[class*="popup"]',
            '[class*="overlay"]',
            '[class*="dialog"]',
            '[class*="create-home-evaluation"]',
            '[role="dialog"]',
            '[role="modal"]'
          ];
          
          for (const selector of modalSelectors) {
            const elements = await page.locator(selector).count();
            if (elements > 0) {
              log.info(`🔍 Found ${elements} element(s) matching: ${selector}`);
              
              // Try to screenshot the modal if visible
              try {
                const modalElement = page.locator(selector).first();
                if (await modalElement.isVisible()) {
                  const modalPng = await modalElement.screenshot();
                  const modalKey = `modal-${selector.replace(/[^a-zA-Z0-9]/g, '-')}-${key}-${Date.now()}.png`;
                  await Actor.setValue(modalKey, modalPng, { contentType: 'image/png' });
                  log.info(`📸 Modal screenshot saved: ${modalKey}`);
                  
                  // Get modal text content
                  const modalText = await modalElement.textContent();
                  log.info(`📝 Modal text (${selector}): ${modalText?.substring(0, 200)}...`);
                }
              } catch (e) {
                log.debug(`Could not screenshot modal ${selector}: ${e.message}`);
              }
            }
          }
          
          // Log all visible text on the page for debugging
          const bodyText = await page.textContent('body');
          log.info(`📄 Page text length: ${bodyText?.length || 0} characters`);
          
          // Look for certificate-related keywords in the page
          const keywords = ['FH', 'FEH', 'FORTIFIED', 'Approved', 'Expiration', 'Building Address', 'Download', 'Certificate'];
          for (const keyword of keywords) {
            const count = (bodyText?.match(new RegExp(keyword, 'gi')) || []).length;
            if (count > 0) {
              log.info(`🔍 Found "${keyword}" ${count} times on page`);
            }
          }
          
        } catch (screenshotError) {
          log.warning(`Screenshot failed: ${screenshotError.message}`);
        }
        
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

      // === Extract Certificate Information ===
      log.info('📅 Extracting certificate information...');
      
      // ALWAYS wait and take comprehensive screenshots regardless of debug mode
      await page.waitForTimeout(2000);
      
      log.info('📸 Taking post-search comprehensive analysis...');
      try {
        // Take another screenshot to see current state
        const currentPng = await page.screenshot({ fullPage: true });
        const currentKey = `extraction-state-${key}-${Date.now()}.png`;
        await Actor.setValue(currentKey, currentPng, { contentType: 'image/png' });
        log.info(`📸 Current state screenshot: ${currentKey}`);
        
        // Check what's currently visible
        const currentUrl = page.url();
        const currentTitle = await page.title();
        log.info(`🌐 Current URL: ${currentUrl}`);
        log.info(`📄 Current Title: ${currentTitle}`);
        
        // Look for the specific modal container
        const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
        const modalExists = await modalContainer.count() > 0;
        
        log.info(`📦 Modal container exists: ${modalExists}`);
        
        if (modalExists) {
          const isVisible = await modalContainer.isVisible();
          log.info(`👁️ Modal visible: ${isVisible}`);
          
          if (isVisible) {
            try {
              const modalPng = await modalContainer.screenshot();
              const modalKey = `modal-container-${key}-${Date.now()}.png`;
              await Actor.setValue(modalKey, modalPng, { contentType: 'image/png' });
              log.info(`📸 Modal container screenshot: ${modalKey}`);
            } catch (e) {
              log.warning(`Could not screenshot modal: ${e.message}`);
            }
          }
        } else {
          // If no modal found, let's look for any containers with certificate info
          log.info('🔍 No modal found, searching for alternative containers...');
          
          const alternativeSelectors = [
            '[class*="evaluation"]',
            '[class*="certificate"]',
            '[class*="info"]',
            '[class*="details"]',
            '[class*="container"]',
            'table',
            '.content',
            '#content',
            'main'
          ];
          
          for (const selector of alternativeSelectors) {
            const elements = await page.locator(selector).count();
            if (elements > 0) {
              log.info(`🔍 Found ${elements} elements matching: ${selector}`);
              
              try {
                const element = page.locator(selector).first();
                const text = await element.textContent();
                if (text && text.length > 50) {
                  log.info(`📝 Content in ${selector}: ${text.substring(0, 150)}...`);
                  
                  // Check if it contains certificate-related keywords
                  const keywords = ['FH', 'Approved', 'Expiration', 'Building'];
                  const hasKeywords = keywords.some(keyword => 
                    text.toLowerCase().includes(keyword.toLowerCase())
                  );
                  
                  if (hasKeywords) {
                    log.info(`✅ ${selector} contains certificate keywords!`);
                    try {
                      const elementPng = await element.screenshot();
                      const elementKey = `potential-container-${selector.replace(/[^a-zA-Z0-9]/g, '-')}-${key}-${Date.now()}.png`;
                      await Actor.setValue(elementKey, elementPng, { contentType: 'image/png' });
                      log.info(`📸 Potential container screenshot: ${elementKey}`);
                    } catch (e) {
                      log.debug(`Could not screenshot ${selector}: ${e.message}`);
                    }
                  }
                }
              } catch (e) {
                log.debug(`Could not analyze ${selector}: ${e.message}`);
              }
            }
          }
        }
        
      } catch (e) {
        log.warning(`Comprehensive analysis failed: ${e.message}`);
      }
      
      // Use enhanced debug for modal state if debug mode is on
      if (debug) await debugModalState(page, 'CERTIFICATE_INFO_EXTRACTION');
      
      // Verify we have the correct address loaded by checking the modal content
      try {
        const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
        if (await modalContainer.count() > 0) {
          const modalContent = await modalContainer.textContent();
          const addressParts = addr.toLowerCase().split(/\s+/);
          const streetNumber = addressParts[0];
          
          // Check if the modal contains our street number
          if (!modalContent.toLowerCase().includes(streetNumber)) {
            log.warning(`⚠️ Warning: Street number ${streetNumber} not found in modal content`);
            log.warning('This might be the wrong address!');
            
            if (debug) await debugModalState(page, 'POSSIBLE_WRONG_ADDRESS');
          } else {
            log.info(`✅ Street number ${streetNumber} confirmed in modal content`);
          }
        }
      } catch (e) {
        log.debug(`Could not verify address in modal: ${e.message}`);
      }
      
      // Extract FH/FEH number from modal
      const fhNumber = await extractFHNumber(page);
      log.info(`🔢 FH/FEH Number: ${fhNumber || 'Not found'}`);
      
      // Extract all certificate information (dates + building address) from modal
      const certInfo = await extractCertificateInfo(page);
      
      log.info('📋 Certificate Information Summary:');
      log.info(`   FH Number: ${fhNumber || 'NOT FOUND'}`);
      log.info(`   Building Address: ${certInfo.buildingAddress || 'NOT FOUND'}`);
      log.info(`   Building City: ${certInfo.buildingCity || 'NOT FOUND'}`);
      log.info(`   Building State: ${certInfo.buildingState || 'NOT FOUND'}`);
      log.info(`   Approved At: ${certInfo.approvedAt || 'NOT FOUND'}`);
      log.info(`   Expiration Date: ${certInfo.expirationDate || 'NOT FOUND'}`);

      // Look for Download button or certificate download option within the modal
      log.info('🔍 Looking for certificate download option in modal...');
      await page.waitForTimeout(1000);

      // Try to find download button within the modal first, then fallback to page
      const modalContainer = page.locator('[class*="create-home-evaluation-info-container"]');
      let downloadButton = modalContainer.getByText(/^\s*Download\s*$/i).first();
      
      if (!(await downloadButton.count())) {
        // Fallback to page-level download button
        downloadButton = page.getByText(/^\s*Download\s*$/i).first();
      }
      
      const hasDownload = await downloadButton.count();

      if (!hasDownload) {
        log.warning('No Download button found in modal or page');
        
        if (debug) await debugModalState(page, 'NO_DOWNLOAD_BUTTON');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'no_certificate',
            error: 'No download button found',
            fhNumber: fhNumber || null,
            buildingAddress: certInfo.buildingAddress || null,
            buildingCity: certInfo.buildingCity || null,
            buildingState: certInfo.buildingState || null,
            buildingZip: certInfo.buildingZip || null,
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
        
        // Go back to main page for next iteration
        await page.goto(loginUrl, { waitUntil: 'networkidle' });
        continue;
      }

      log.info('✅ Found Download button');

      // Download the certificate
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
        log.info('Download event detected; reading file…');
        try {
          const stream = await dl.createReadStream();
          if (stream) buffer = await streamToBuffer(stream);
          else {
            const filePath = await dl.path();
            buffer = await fs.readFile(filePath);
          }
        } catch (e) {
          log.warning(`Stream read failed: ${e.message}`);
        }
      } else if (signal?.kind === 'response') {
        log.info('Inline PDF response detected; capturing body…');
        buffer = await signal.r.body();
      } else if (signal?.kind === 'popup') {
        log.info('Popup detected; trying to capture PDF from popup…');
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

      const approvedAt = certInfo.approvedAt || '';
      const expirationDate = certInfo.expirationDate || '';
      const buildingAddress = certInfo.buildingAddress || '';
      
      const basePretty = `${fhNumber || key}${expirationDate ? ` - Expires ${expirationDate}` : ''}`.replace(/\s+/g, ' ');
      const prettyName = sanitizeFileName(`${basePretty}.pdf`);
      const kvName = kvSafeKey(`${fhNumber || key}-certificate.pdf`);

      if (!buffer || buffer.length === 0) {
        log.warning(`PDF buffer empty for ${key}; not saving.`);
      } else {
        await Actor.setValue(kvName, buffer, { contentType: 'application/pdf' });
        log.info(`Saved to KVS: ${kvName} (${buffer.length} bytes)`);
      }

      try {
        const repoDownloads = path.join(process.cwd(), 'downloads');
        await fs.mkdir(repoDownloads, { recursive: true });
        if (buffer && buffer.length > 0) {
          await fs.writeFile(path.join(repoDownloads, prettyName), buffer);
          log.info(`Saved (repo): ${path.join(repoDownloads, prettyName)}`);
        }
      } catch (e) {
        log.warning(`Could not save to ./downloads: ${e.message}`);
      }

      try {
        const userDownloads = path.join(process.env.USERPROFILE || '', 'Downloads');
        if (userDownloads && buffer && buffer.length > 0) {
          await fs.mkdir(userDownloads, { recursive: true });
          await fs.writeFile(path.join(userDownloads, prettyName), buffer);
          log.info(`Saved (Windows Downloads): ${path.join(userDownloads, prettyName)}`);
        }
      } catch (e) {
        // Cloud won't have USERPROFILE
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
          } else {
            log.warning('Google Drive upload skipped or failed (no file id returned).');
          }
        } else {
          log.warning('Skipping Drive upload (empty buffer).');
        }
      } catch (e) {
        log.warning(`Google Drive upload error: ${e.message}`);
      }

      const downloadStatus = buffer && buffer.length > 0 ? 'downloaded' : 'empty_pdf';
      
      if (returnStructuredData) {
        await Actor.pushData({
          // Input data
          address: addr,
          searchAddress: key,
          
          // Status
          status: downloadStatus,
          success: true,
          
          // Certificate details
          fhNumber: fhNumber || null,
          buildingAddress: buildingAddress || null,
          buildingCity: certInfo.buildingCity || null,
          buildingState: certInfo.buildingState || null,
          buildingZip: certInfo.buildingZip || null,
          approvedAt: approvedAt || null,
          expirationDate: expirationDate || null,
          
          // File information
          certificateFile: kvName,
          fileName: prettyName,
          fileSize: buffer ? buffer.length : 0,
          
          // Google Drive links (if uploaded)
          googleDriveFileId: driveFileId,
          googleDriveLink: driveWebViewLink,
          
          // Download URLs for n8n to fetch
          apifyDownloadUrl: driveFileId ? null : `https://api.apify.com/v2/key-value-stores/${Actor.getEnv().defaultKeyValueStoreId}/records/${kvName}`,
          
          // Timestamps
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
        buildingAddress: buildingAddress || null,
        fileSize: buffer ? buffer.length : 0,
        approvedAt: approvedAt || null,
        expirationDate: expirationDate || null,
      };
      await saveProcessed(processed);

      // Go back to main page for next address
      log.info('🔙 Returning to main page...');
      await page.goto(loginUrl, { waitUntil: 'networkidle' });
      await page.waitForTimeout(1000);

      handled++;
    }

    log.info(`Run complete. Addresses handled: ${handled}`);
    
    // Set output for easy n8n access
    await Actor.setOutput({
      summary: {
        totalAddresses: addresses.length,
        processedAddresses: handled,
        timestamp: new Date().toISOString()
      }
    });
    
  } finally {
    await browser.close().catch(() => {});
    await Actor.exit();
  }
}

Actor.main(run);