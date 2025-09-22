// src/main.js - Complete Updated Version with Fixed Result Clicking
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
    
    if (step !== 'QUICK_CHECK') {
      // Take screenshot only for important steps
      const png = await page.screenshot({ fullPage: true });
      const screenshotKey = `debug-${step.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.png`;
      await Actor.setValue(screenshotKey, png, { contentType: 'image/png' });
      log.info(`📸 Screenshot saved as: ${screenshotKey}`);
    }
    
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

// === GOOGLE DRIVE INTEGRATION ===
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
  
  try {
    await jwt.authorize();
    const drive = google.drive({ version: 'v3', auth: jwt });
    const media = { mimeType, body: Readable.from(buffer) };

    const res = await drive.files.create({
      requestBody: { 
        name: fileName, 
        parents: [folderId],
        description: `IBHS Certificate - Generated ${new Date().toISOString()}`
      },
      media,
      fields: 'id, webViewLink, webContentLink, name, size',
    });
    
    log.info(`✅ Google Drive upload successful: ${res.data.name}`);
    return res.data;
  } catch (error) {
    log.error(`❌ Google Drive upload failed: ${error.message}`);
    throw error;
  }
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

// === UPDATED SEARCH AND EXTRACT FUNCTION ===
async function searchAndExtractCertificate(page, address, { politeDelayMs, debug, returnStructuredData }) {
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

  // OPEN FIRST RESULT - IMPROVED VERSION
  await debugPageState(page, 'SEARCH_RESULTS_FOUND');

  // Wait for results to stabilize
  await page.waitForTimeout(3000);

  // Check if we have any results
  const hasResults = await Promise.race([
    page.locator('tr, [role="row"]').count().then(count => count > 1),
    page.locator('text=/no results/i, text=/not found/i').count().then(count => count === 0)
  ]);

  if (!hasResults) {
    throw new Error('No search results found for this address');
  }

  // Try multiple strategies to access the certificate
  let accessSuccessful = false;
  const streetNumber = address.split(',')[0].trim();

  // Strategy 1: Look for direct download button (sometimes results show download immediately)
  const directDownload = page.locator('text=/download/i, [title*="download" i]').first();
  if (await directDownload.count() > 0 && await directDownload.isVisible()) {
    log.info('📥 Found direct download button in results');
    accessSuccessful = true;
  }

  if (!accessSuccessful) {
    // Strategy 2: Click on the address text
    const addressLink = page.getByText(streetNumber, { exact: false }).first();
    if (await addressLink.count() > 0 && await addressLink.isVisible()) {
      try {
        log.info('📍 Clicking on address link...');
        await addressLink.click({ timeout: 10000 });
        await page.waitForLoadState('networkidle', { timeout: 30000 });
        accessSuccessful = true;
      } catch (e) {
        log.info(`Address click failed: ${e.message}`);
      }
    }
  }

  if (!accessSuccessful) {
    // Strategy 3: Click on certificate ID if visible
    const certElements = await page.locator('text=/FH\\d+/, [title*="FH"], [data-testid*="fortified"]').all();
    for (const cert of certElements) {
      if (await cert.isVisible()) {
        try {
          log.info('🆔 Clicking on certificate ID...');
          await cert.click({ timeout: 10000 });
          await page.waitForLoadState('networkidle', { timeout: 30000 });
          accessSuccessful = true;
          break;
        } catch (e) {
          continue;
        }
      }
    }
  }

  if (!accessSuccessful) {
    // Strategy 4: Try clicking on table cells that contain our address
    const tableCells = page.locator('td, th').filter({ hasText: streetNumber });
    const cellCount = await tableCells.count();
    
    for (let i = 0; i < cellCount; i++) {
      try {
        const cell = tableCells.nth(i);
        if (await cell.isVisible()) {
          log.info(`📋 Clicking on table cell ${i + 1}...`);
          await cell.click({ timeout: 10000 });
          await page.waitForLoadState('networkidle', { timeout: 30000 });
          accessSuccessful = true;
          break;
        }
      } catch (e) {
        continue;
      }
    }
  }

  if (!accessSuccessful) {
    // Strategy 5: Click anywhere in the row containing our address
    const rows = page.locator('tr, [role="row"]').filter({ hasText: streetNumber });
    const rowCount = await rows.count();
    
    for (let i = 0; i < Math.min(rowCount, 3); i++) {
      try {
        const row = rows.nth(i);
        if (await row.isVisible()) {
          log.info(`📄 Clicking on result row ${i + 1}...`);
          
          // Try clicking on different parts of the row
          const clickableElements = [
            row.locator('a').first(),
            row.locator('button').first(), 
            row.locator('td').first(),
            row
          ];
          
          for (const element of clickableElements) {
            try {
              if (await element.count() > 0 && await element.isVisible()) {
                await element.click({ timeout: 8000 });
                await page.waitForLoadState('networkidle', { timeout: 30000 });
                accessSuccessful = true;
                break;
              }
            } catch (e) {
              continue;
            }
          }
          
          if (accessSuccessful) break;
        }
      } catch (e) {
        continue;
      }
    }
  }

  await debugPageState(page, 'AFTER_RESULT_INTERACTION');

  // Check if we now have access to certificate details
  const hasCertificateAccess = await Promise.race([
    page.locator('text=/download/i').count().then(count => count > 0),
    page.locator('text=/certificate/i').count().then(count => count > 0),
    page.locator('[role="tab"]').filter({ hasText: /certificate/i }).count().then(count => count > 0)
  ]);

  if (!hasCertificateAccess) {
    throw new Error('Could not access certificate details - no download or certificate section found');
  }

  // NAVIGATE TO CERTIFICATES SECTION (if needed)
  let certControl = page.getByText(/^\s*Certificates?\s*$/i).first();
  if (!(await certControl.count()) || !(await certControl.isVisible())) {
    certControl = page.locator('[role="tab"], [role="link"], button, a')
      .filter({ hasText: /Certificates?/i })
      .first();
  }

  if (await certControl.count() && await certControl.isVisible()) {
    log.info('🏛️ Navigating to Certificate section...');
    await certControl.click();
    await page.waitForLoadState('networkidle', { timeout: 60_000 });
    await sleep(jitter(300));
  }

  await debugPageState(page, 'CERTIFICATE_SECTION');

  // EXTRACT CERTIFICATE DATA
  const certificateData = {
    address: address,
    searchKey: key,
    timestamp: new Date().toISOString(),
    status: 'found',
    certificateDetails: {},
    downloadInfo: null,
    googleDriveInfo: null
  };

  // Extract certificate details
  try {
    // Look for expiration date
    const expElements = await page.locator('text=/expires/i, text=/expiration/i').all();
    for (const exp of expElements) {
      if (await exp.isVisible()) {
        const expText = await exp.textContent();
        certificateData.certificateDetails.expirationText = expText?.trim();
        break;
      }
    }

    // Look for certificate ID
    const certIdElements = await page.locator('text=/FH\\d+/, text=/certificate/i').allTextContents();
    if (certIdElements.length > 0) {
      certificateData.certificateDetails.certificateReferences = certIdElements;
    }

    // Look for designation (Roof, etc.)
    const designationElements = await page.locator('text=/roof/i, text=/designation/i').allTextContents();
    if (designationElements.length > 0) {
      certificateData.certificateDetails.designation = designationElements[0];
    }

  } catch (e) {
    log.warning(`Could not extract all certificate details: ${e.message}`);
  }

  // DOWNLOAD CERTIFICATE
  const downloadButton = page.locator('text=/download/i, [title*="download" i], button:has-text("Download")').first();
  
  if (!(await downloadButton.count()) || !(await downloadButton.isVisible())) {
    throw new Error('Download button not found or not visible');
  }

  log.info('📥 Starting certificate download...');
  
  // Set up download handlers
  const downloadPromise = page.waitForEvent('download', { timeout: 120_000 }).then(d => ({ kind: 'download', d })).catch(() => null);
  const responsePromise = page.waitForResponse(
    resp => (resp.headers()['content-type'] || '').toLowerCase().includes('application/pdf'),
    { timeout: 120_000 }
  ).then(r => ({ kind: 'response', r })).catch(() => null);

  await downloadButton.click();

  const signal = await Promise.race([downloadPromise, responsePromise]);
  if (!signal) {
    // Wait a bit more and try again
    await page.waitForTimeout(5000);
    const signal2 = await Promise.race([downloadPromise, responsePromise]);
    if (!signal2) {
      throw new Error('Download did not start after clicking download button');
    }
    var actualSignal = signal2;
  } else {
    var actualSignal = signal;
  }

  let buffer = null;
  if (actualSignal.kind === 'download') {
    const stream = await actualSignal.d.createReadStream();
    buffer = stream ? await streamToBuffer(stream) : await fs.readFile(await actualSignal.d.path());
  } else if (actualSignal.kind === 'response') {
    buffer = await actualSignal.r.body();
  }

  if (!buffer || buffer.length === 0) {
    throw new Error('Downloaded file is empty or could not be read');
  }

  log.info(`✅ Certificate downloaded: ${buffer.length} bytes`);

  // UPLOAD TO GOOGLE DRIVE
  const fileName = sanitizeFileName(`IBHS_Certificate_${key}_${Date.now()}.pdf`);
  
  try {
    const driveFile = await uploadToGoogleDrive(buffer, fileName);
    if (driveFile) {
      certificateData.googleDriveInfo = {
        fileId: driveFile.id,
        fileName: driveFile.name,
        webViewLink: driveFile.webViewLink,
        webContentLink: driveFile.webContentLink,
        size: driveFile.size,
        uploadedAt: new Date().toISOString()
      };
      log.info(`✅ Uploaded to Google Drive: ${driveFile.webViewLink}`);
    }
  } catch (e) {
    log.warning(`Google Drive upload failed: ${e.message}`);
    certificateData.googleDriveInfo = { error: e.message };
  }

  // SAVE TO APIFY STORAGE (backup)
  const kvName = kvSafeKey(`${key}-certificate.pdf`);
  await Actor.setValue(kvName, buffer, { contentType: 'application/pdf' });
  
  certificateData.downloadInfo = {
    fileName: fileName,
    apifyKey: kvName,
    fileSize: buffer.length,
    downloadedAt: new Date().toISOString()
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
    returnStructuredData = true,
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
    throw new Error('No address provided. Use "address" field for single address.');
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
    const result = await searchAndExtractCertificate(page, targetAddress, { politeDelayMs, debug, returnStructuredData });
    
    log.info(`🎉 Successfully processed: ${targetAddress}`);
    
    // Push structured data to dataset for n8n to consume
    if (returnStructuredData) {
      await Actor.pushData(result);
      log.info('📊 Structured data pushed to dataset');
    }

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
      downloadInfo: null,
      googleDriveInfo: null
    };

    if (returnStructuredData) {
      await Actor.pushData(errorResult);
    }
    
    await Actor.setValue('latest-result', errorResult);
    
    throw error;
  } finally {
    await browser.close();
    await Actor.exit();
  }
}

Actor.main(run);
