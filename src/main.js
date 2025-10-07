// src/main.js - CORRECTED VERSION - Wait for popup data to load
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

async function captureAndSaveScreenshot(page, address, stage = 'final') {
  try {
    const timestamp = Date.now();
    const safeName = sanitizeFileName(address);
    const screenshotKey = `screenshot-${safeName}-${stage}-${timestamp}.png`;
    
    log.info(`📸 Capturing ${stage} screenshot for: ${address}`);
    
    const png = await page.screenshot({ 
      fullPage: true,
      timeout: 30000 
    });
    
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

// ==================== DATA EXTRACTION FROM POPUP ====================

async function extractCertificateDataFromPopup(page, address) {
  const data = {
    fhNumber: null,
    approvedAt: null,
    expirationDate: null,
    buildingAddress: null,
    buildingCity: null,
    buildingZip: null,
    designation: null,
    program: null,
    status: null
  };

  try {
    log.info('');
    log.info('═'.repeat(70));
    log.info('🔍 EXTRACTING DATA FROM POPUP MODAL');
    log.info('═'.repeat(70));
    log.info('');
    
    // Wait for the popup content to change from search form to certificate details
    // The key is waiting for specific certificate fields to appear
    log.info('⏳ Waiting for certificate details to load in popup...');
    
    // Wait for one of these elements that should appear in the certificate view
    const certificateIndicators = [
      'text=/Program/i',
      'text=/Designation Level/i',
      'text=/Approved/i',
      'text=/Expiration/i',
      'text=/FH\\d+/i',
      'text=/FEH\\d+/i'
    ];
    
    let popupLoaded = false;
    for (const indicator of certificateIndicators) {
      try {
        await page.waitForSelector(indicator, { timeout: 15000 });
        log.info(`✅ Found certificate indicator: ${indicator}`);
        popupLoaded = true;
        break;
      } catch (e) {
        log.info(`   Indicator not found: ${indicator}`);
      }
    }
    
    if (!popupLoaded) {
      log.warning('⚠️ Certificate details may not have loaded in popup');
    }
    
    // Wait a bit more for all content to render
    await page.waitForTimeout(5000);
    
    // Capture the popup HTML for debugging
    const modalSelectors = [
      '#home-create-evaluation-dialog',
      '[class*="create-home-evaluation"]',
      '[class*="evaluation-dialog"]',
      '.e-dlg-content',
      '[role="dialog"]'
    ];
    
    let modalHtml = null;
    for (const selector of modalSelectors) {
      try {
        const element = page.locator(selector).first();
        if (await element.count() > 0) {
          modalHtml = await element.innerHTML();
          log.info(`✅ Captured HTML from: ${selector}`);
          break;
        }
      } catch (e) {
        // Continue
      }
    }
    
    if (modalHtml) {
      // Save HTML for inspection
      const htmlKey = `debug-popup-${sanitizeFileName(address)}-${Date.now()}.html`;
      await Actor.setValue(htmlKey, modalHtml, { contentType: 'text/html' });
      const kvStoreId = Actor.getEnv().defaultKeyValueStoreId;
      const htmlUrl = `https://api.apify.com/v2/key-value-stores/${kvStoreId}/records/${htmlKey}`;
      log.info(`💾 Popup HTML saved: ${htmlUrl}`);
      
      log.info(`📄 HTML Preview (first 2000 chars):`);
      log.info(modalHtml.substring(0, 2000));
      log.info('...');
    }
    
    // Get all text content from the popup
    const popupText = await page.locator('[role="dialog"]').first().textContent().catch(() => '');
    log.info('');
    log.info('📝 Full popup text content:');
    log.info(popupText);
    log.info('');
    
    // Try to find data in the popup using various strategies
    log.info('🔍 Attempting data extraction...');
    
    // Strategy 1: Look for table within the popup
    const popupTables = await page.locator('[role="dialog"] table').all();
    log.info(`Found ${popupTables.length} table(s) in popup`);
    
    for (let i = 0; i < popupTables.length; i++) {
      const table = popupTables[i];
      const rows = await table.locator('tr').all();
      log.info(`\nTable ${i + 1} in popup:`);
      
      for (let j = 0; j < rows.length; j++) {
        const cells = await rows[j].locator('td, th').all();
        const cellTexts = [];
        
        for (const cell of cells) {
          const text = await cell.textContent();
          cellTexts.push(text?.trim() || '');
        }
        
        if (cellTexts.length > 0) {
          log.info(`   Row ${j + 1}: [${cellTexts.join(' | ')}]`);
          
          // Try to extract data from this row
          const rowText = cellTexts.join(' ');
          
          // Look for FH/FEH number
          const fhMatch = rowText.match(/FE?H[\s:-]?\d{8,}/i);
          if (fhMatch && !data.fhNumber) {
            data.fhNumber = fhMatch[0].replace(/[\s:-]/g, '').toUpperCase();
            log.info(`   ✓ Found FH Number: ${data.fhNumber}`);
          }
          
          // Look for dates
          const dateMatches = rowText.match(/\d{1,2}[\/-]\d{1,2}[\/-]\d{4}/g);
          if (dateMatches) {
            // Try to determine which date is which based on nearby text
            for (let k = 0; k < cellTexts.length; k++) {
              const cellText = cellTexts[k];
              const prevCell = k > 0 ? cellTexts[k - 1] : '';
              
              if (/approved/i.test(prevCell) || /approved/i.test(cellText)) {
                const dateMatch = cellText.match(/\d{1,2}[\/-]\d{1,2}[\/-]\d{4}/);
                if (dateMatch && !data.approvedAt) {
                  data.approvedAt = dateMatch[0];
                  log.info(`   ✓ Found Approved Date: ${data.approvedAt}`);
                }
              }
              
              if (/expir/i.test(prevCell) || /expir/i.test(cellText)) {
                const dateMatch = cellText.match(/\d{1,2}[\/-]\d{1,2}[\/-]\d{4}/);
                if (dateMatch && !data.expirationDate) {
                  data.expirationDate = dateMatch[0];
                  log.info(`   ✓ Found Expiration Date: ${data.expirationDate}`);
                }
              }
            }
          }
        }
      }
    }
    
    // Strategy 2: Look for labeled fields in the popup
    const fieldLabels = [
      { label: 'Program', field: 'program' },
      { label: 'Designation Level', field: 'designation' },
      { label: 'Building Address', field: 'buildingAddress' },
      { label: 'Building City', field: 'buildingCity' },
      { label: 'Building Zip', field: 'buildingZip' },
      { label: 'Status', field: 'status' }
    ];
    
    for (const { label, field } of fieldLabels) {
      try {
        // Find the label element within the dialog
        const labelElement = page.locator('[role="dialog"]').locator(`text=${label}`).first();
        
        if (await labelElement.count() > 0) {
          // Try to find the value near the label
          const parent = labelElement.locator('xpath=ancestor::tr[1]');
          
          if (await parent.count() > 0) {
            const parentText = await parent.textContent();
            // Remove the label from the text to get the value
            const value = parentText.replace(label, '').trim();
            
            if (value && value.length > 0 && value.length < 200) {
              data[field] = value;
              log.info(`   ✓ Found ${label}: ${value}`);
            }
          }
        }
      } catch (e) {
        // Continue
      }
    }
    
    // Strategy 3: Use regex patterns on the full popup text
    if (!data.fhNumber) {
      const fhMatch = popupText.match(/FE?H[\s:-]?\d{8,}/i);
      if (fhMatch) {
        data.fhNumber = fhMatch[0].replace(/[\s:-]/g, '').toUpperCase();
        log.info(`   ✓ Found FH Number in text: ${data.fhNumber}`);
      }
    }
    
    if (!data.approvedAt) {
      const approvedMatch = popupText.match(/Approved[^0-9]*(\d{1,2}[\/-]\d{1,2}[\/-]\d{4})/i);
      if (approvedMatch) {
        data.approvedAt = approvedMatch[1];
        log.info(`   ✓ Found Approved Date in text: ${data.approvedAt}`);
      }
    }
    
    if (!data.expirationDate) {
      const expirationMatch = popupText.match(/Expir[^0-9]*(\d{1,2}[\/-]\d{1,2}[\/-]\d{4})/i);
      if (expirationMatch) {
        data.expirationDate = expirationMatch[1];
        log.info(`   ✓ Found Expiration Date in text: ${data.expirationDate}`);
      }
    }
    
    log.info('');
    log.info('═'.repeat(70));
    log.info('📊 EXTRACTION SUMMARY');
    log.info('═'.repeat(70));
    log.info(`   FH Number:        ${data.fhNumber || '❌ NOT FOUND'}`);
    log.info(`   Approved At:      ${data.approvedAt || '❌ NOT FOUND'}`);
    log.info(`   Expiration Date:  ${data.expirationDate || '❌ NOT FOUND'}`);
    log.info(`   Building Address: ${data.buildingAddress || '❌ NOT FOUND'}`);
    log.info(`   Program:          ${data.program || '❌ NOT FOUND'}`);
    log.info(`   Designation:      ${data.designation || '❌ NOT FOUND'}`);
    log.info('═'.repeat(70));
    log.info('');
    
    return data;
    
  } catch (e) {
    log.error(`❌ Popup extraction error: ${e.message}`);
    log.error(`Stack: ${e.stack}`);
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
        log.info('✅ Step 4: Selecting search result (pressing Enter)...');
        await page.keyboard.press('Enter');
        
        // CRITICAL: Wait longer for the popup to load the certificate details
        log.info('⏳ Step 5: Waiting for popup to load certificate details...');
        await page.waitForTimeout(10000); // Wait 10 seconds for data to load
        await page.waitForLoadState('networkidle', { timeout: 60000 });

        // Capture screenshot BEFORE extraction
        log.info('📸 Step 6a: Capturing screenshot of loaded popup...');
        const screenshotBefore = await captureAndSaveScreenshot(page, addr, 'popup-loaded');
        result.screenshotBefore = screenshotBefore.url;

        // Extract data from the popup
        log.info('📊 Step 6b: Extracting data from popup...');
        const popupData = await extractCertificateDataFromPopup(page, addr);
        
        result.fhNumber = popupData.fhNumber;
        result.approvedAt = popupData.approvedAt;
        result.expirationDate = popupData.expirationDate;
        result.buildingAddress = popupData.buildingAddress;
        result.program = popupData.program;
        result.designation = popupData.designation;
        result.status = popupData.status;

        // Capture screenshot AFTER extraction
        log.info('📸 Step 6c: Capturing final screenshot...');
        const screenshot = await captureAndSaveScreenshot(page, addr, 'after-extraction');
        result.screenshot = screenshot.url;
        result.screenshotKey = screenshot.key;

        // Try to download PDF
        log.info('📥 Step 7: Attempting PDF download...');
        const downloadButton = page.locator('[role="dialog"]').getByText(/^\s*Download\s*$/i).first();
        
        if (await downloadButton.count() > 0) {
          const downloadPromise = page.waitForEvent('download', { timeout: 60_000 })
            .then(d => ({ kind: 'download', d }))
            .catch(() => null);

          // Try to click with force if needed
          try {
            await downloadButton.click({ timeout: 5000 });
          } catch (clickError) {
            log.warning(`⚠️ Normal click failed, trying force click: ${clickError.message}`);
            await downloadButton.click({ force: true });
          }
          
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
          } else {
            log.warning('⚠️ Download event did not trigger');
          }
        } else {
          log.warning('⚠️ Download button not found in popup');
        }

        result.success = true;
        result.status = 'completed';

      } catch (error) {
        log.error(`❌ Error processing ${addr}: ${error.message}`);
        log.error(`Stack: ${error.stack}`);
        result.error = error.message;
        result.errorStack = error.stack;
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
      log.info(`   FH Number:        ${result.fhNumber || '❌ NOT FOUND'}`);
      log.info(`   Approved At:      ${result.approvedAt || '❌ NOT FOUND'}`);
      log.info(`   Expiration Date:  ${result.expirationDate || '❌ NOT FOUND'}`);
      log.info(`   Building Address: ${result.buildingAddress || '❌ NOT FOUND'}`);
      log.info(`   Screenshot:       ${result.screenshot ? '✅ SAVED' : '❌ FAILED'}`);
      log.info(`   Certificate:      ${result.certificateFile ? '✅ DOWNLOADED' : '⚠️ NOT AVAILABLE'}`);
      log.info('');

      // Close any open dialogs before moving to next address
      try {
        const closeButton = page.locator('[role="dialog"] button[aria-label="Close"]').first();
        if (await closeButton.count() > 0) {
          await closeButton.click();
          await page.waitForTimeout(1000);
        }
      } catch (e) {
        // Continue
      }

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