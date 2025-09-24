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
  const appHint   = page.waitForSelector('text=/Certificates?|Search/i', { timeout: 30_000 }).catch(() => null);
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

async function run() {
  await Actor.init();

  const input = (await Actor.getInput()) || {};
  const {
    loginUrl = 'https://app.ibhs.org/fh',
    addresses: rawAddresses = [],
    address,
    maxAddressesPerRun = 1,
    politeDelayMs = 800,
    debug = false,
    returnStructuredData = true,
    username: usernameFromInput,
    password: passwordFromInput,
  } = input;

  let addresses = [];
  
  if (address && typeof address === 'string') {
    addresses = [address.trim()];
  } else if (Array.isArray(rawAddresses)) {
    addresses = rawAddresses;
  } else if (typeof rawAddresses === 'string') {
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

      await debugPageState(page, 'BEFORE_SEARCH');

      log.info(`🔍 Searching for: ${addr}`);

      log.info('⏳ Waiting for page to be fully loaded...');
      await page.waitForTimeout(3000);
      await page.waitForLoadState('networkidle');

      let searchField = null;
      const strategies = [
        () => page.getByPlaceholder('Search'),
        () => page.locator('input[type="search"][placeholder="Search"]'),
        () => page.locator('#grid_1544421861_0_searchbar'),
        () => page.locator('input[type="search"]:not([aria-label="clipboard"])'),
        () => page.locator('[placeholder*="search" i]:not([aria-label="clipboard"])'),
        () => page.locator('[aria-label*="search" i]:not([aria-label="clipboard"])'),
      ];

      for (let i = 0; i < strategies.length; i++) {
        try {
          log.info(`🔍 Trying search strategy ${i + 1}...`);
          const element = strategies[i]();
          
          await element.waitFor({ state: 'visible', timeout: 5000 }).catch(() => {});
          
          const count = await element.count();
          
          if (count > 0) {
            const isVisible = await element.isVisible();
            const isEnabled = await element.isEnabled();
            log.info(`  Found ${count} elements, visible: ${isVisible}, enabled: ${isEnabled}`);
            
            if (isVisible && isEnabled) {
              searchField = element;
              log.info(`✅ Using search strategy ${i + 1}`);
              break;
            }
          }
        } catch (e) {
          log.info(`  Strategy ${i + 1} failed: ${e.message}`);
        }
      }

      if (!searchField) {
        await debugPageState(page, 'NO_SEARCH_FIELD');
        log.warning('Search field not found; reloading shell once.');
        await page.reload({ waitUntil: 'networkidle' });
        await sleep(jitter(politeDelayMs));
        
        searchField = page.getByPlaceholder('Search');
        if (!(await searchField.count())) {
          await debugPageState(page, 'STILL_NO_SEARCH_FIELD');
          
          if (returnStructuredData) {
            await Actor.pushData({
              address: addr,
              searchAddress: key,
              status: 'no_search_field',
              error: 'Could not find search field on page',
              processedAt: new Date().toISOString()
            });
          }
          
          processed[key] = { status: 'no_search_field' };
          await saveProcessed(processed);
          handled++;
          continue;
        }
      }

      try {
        log.info(`📝 Filling search field with: ${addr}`);
        await searchField.click();
        await page.waitForTimeout(500);
        await searchField.fill(addr);
        await sleep(jitter(500));
        
        log.info(`⌨️ Pressing Enter to search...`);
        await page.keyboard.press('Enter');
        
        await page.waitForLoadState('networkidle', { timeout: 60_000 });
        await sleep(jitter(politeDelayMs));
        
        await debugPageState(page, 'AFTER_SEARCH');
        
      } catch (searchError) {
        log.error(`❌ Search failed: ${searchError.message}`);
        await debugPageState(page, 'SEARCH_ERROR');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'search_failed',
            error: `Search failed: ${searchError.message}`,
            processedAt: new Date().toISOString()
          });
        }
        
        processed[key] = { status: 'search_failed', error: searchError.message };
        await saveProcessed(processed);
        handled++;
        continue;
      }

      // Look for Download button directly in search results
      log.info('⏳ Waiting for search results to load...');
      await page.waitForTimeout(2000);

      const downloadButton = page.getByText(/^\s*Download\s*$/i).first();
      const hasDownload = await downloadButton.count();

      if (!hasDownload) {
        log.warning('No Download button found in search results');
        
        if (returnStructuredData) {
          await Actor.pushData({
            address: addr,
            searchAddress: key,
            status: 'no_certificate',
            error: 'No download button found in search results',
            processedAt: new Date().toISOString()
          });
        }
        
        processed[key] = { status: 'no_certificate' };
        await saveProcessed(processed);
        handled++;
        continue;
      }

      log.info('✅ Found Download button in search results');

      // Download the certificate
      if (debug) await snapshot(page, `pre-download-${key}`);

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

      // Try to get expiration date from the table row
      let expires = '';
      try {
        const expiresCell = page.locator('text=/expires/i').first();
        if (await expiresCell.count()) {
          const cellText = await expiresCell.textContent();
          expires = cellText?.trim() || '';
        }
      } catch {}

      const basePretty = `${key}${expires ? ` - Expires ${expires}` : ''}`.replace(/\s+/g, ' ');
      const prettyName = sanitizeFileName(`${basePretty}.pdf`);
      const kvName = kvSafeKey(`${key}-certificate.pdf`);

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
          address: addr,
          searchAddress: key,
          status: downloadStatus,
          certificateFile: kvName,
          fileName: prettyName,
          fileSize: buffer ? buffer.length : 0,
          expires: expires || null,
          googleDriveFileId: driveFileId,
          googleDriveLink: driveWebViewLink,
          downloadedAt: new Date().toISOString(),
          processedAt: new Date().toISOString()
        });
      }

      processed[key] = { 
        status: downloadStatus, 
        file: kvName, 
        when: new Date().toISOString(),
        fileSize: buffer ? buffer.length : 0,
        expires: expires || null
      };
      await saveProcessed(processed);

      await page.waitForTimeout(1000);

      handled++;
    }

    log.info(`Run complete. Addresses handled: ${handled}`);
  } finally {
    await browser.close().catch(() => {});
    await Actor.exit();
  }
}

Actor.main(run);
