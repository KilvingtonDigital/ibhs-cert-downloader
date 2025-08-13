// src/main.js
import { Actor, log } from 'apify';
import { chromium } from 'playwright';
import fs from 'fs/promises';
import path from 'path';
import { Readable } from 'stream';
import { google } from 'googleapis';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const jitter = (base, spread = 350) => base + Math.floor(Math.random() * spread);

// Normalize addresses
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

// --- Google Drive upload via Service Account ---
async function uploadToGoogleDrive(buffer, fileName, mimeType = 'application/pdf') {
  const folderId = process.env.GOOGLE_DRIVE_FOLDER_ID;
  const clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
  let privateKey = process.env.GOOGLE_SERVICE_ACCOUNT_KEY;

  if (!folderId || !clientEmail || !privateKey) {
    log.warning('Google Drive env not fully configured; skipping Drive upload.');
    return null;
  }

  // Apify secrets often store newlines escaped as \n; unescape
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
  return res.data; // { id, webViewLink }
}

// --- Snapshot helpers (short, safe keys for Apify KV) ---
function safeKey(prefix, label, ext) {
  const clean = (label || '')
    .toLowerCase()
    .replace(/[^a-z0-9!._'()\-]+/g, '-') // allowed chars only
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

// --- Search box finder (avoids login fields) ---
async function openSearch(page) {
  const byPlaceholder = page
    .locator('input[type="search"], [placeholder*="search" i], [aria-label*="search" i]')
    .first();
  if (await byPlaceholder.count()) return byPlaceholder;

  const searchBtn = page.getByText(/^\s*search\s*$/i).first();
  if (await searchBtn.count()) {
    await searchBtn.click();
    return page.getByRole('textbox').first();
  }

  // Common shortcut in React shells
  await page.keyboard.press('/');
  return page.getByRole('textbox').first();
}

// --- Robust login ---
async function ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs, debug }) {
  const emailSel  = 'input[type="email"], input[name="email"], input[autocomplete="username"]';
  const passSel   = 'input[type="password"], input[name="password"], input[autocomplete="current-password"]';
  const submitSel = 'button:has-text("Sign in"), button:has-text("Log in"), button[type="submit"], [type="submit"]';

  await page.goto(loginUrl, { waitUntil: 'domcontentloaded', timeout: 90_000 });
  await page.waitForLoadState('networkidle', { timeout: 60_000 });
  if (debug) await snapshot(page, 'login-loaded');

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
  } else if (await page.locator(passSel).count()) {
    await page.press(passSel, 'Enter');
  }

  const loginGone = page.waitForSelector(emailSel, { state: 'detached', timeout: 30_000 }).catch(() => null);
  const appHint   = page.waitForSelector('text=/Certificates?|Search/i', { timeout: 30_000 }).catch(() => null);
  await Promise.race([loginGone, appHint]);

  if (await page.locator(emailSel).count()) {
    if (await page.locator(submitSel).count()) await page.click(submitSel);
    await page.waitForLoadState('networkidle', { timeout: 30_000 });
  }

  if (debug) await snapshot(page, 'post-login');
  if (await page.locator(emailSel).count()) {
    throw new Error('Login did not complete (email field still visible). Check credentials or selector.');
  }

  await sleep(jitter(politeDelayMs));
}

// --- Open first matching result after a search ---
async function openFirstResult(page, addr, { politeDelayMs, debug }) {
  const streetFrag = addr.split(',')[0].trim();
  const safeFrag = streetFrag.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

  const containers = [
    '[role="grid"]',
    '[role="table"]',
    'table',
    '[data-testid*="result"]',
    '[data-test*="result"]',
    '.results, .search-results, .list, .table, .grid'
  ];
  for (const sel of containers) {
    try {
      const found = await page.locator(sel).first().waitFor({ timeout: 6_000 });
      if (found) break;
    } catch {}
  }

  const rowCandidates = page.locator(
    '[role="row"], tr, [data-testid*="row"], [data-test*="row"], .row, .list-item, .MuiDataGrid-row'
  );
  const rowCount = await rowCandidates.count().catch(() => 0);

  const linkCandidates = page.locator('a');
  const linkCount = await linkCandidates.count().catch(() => 0);

  log.info(`Result probe: rows=${rowCount} links=${linkCount}`);

  const exact = page.getByText(new RegExp(`^\\s*${safeFrag}\\b`, 'i')).first();
  if (await exact.count()) {
    await exact.click();
  } else if (rowCount > 1) {
    await rowCandidates.nth(1).click();
  } else if (linkCount > 0) {
    await linkCandidates.first().click();
  } else {
    throw new Error('No clickable search results found.');
  }

  await page.waitForLoadState('networkidle', { timeout: 60_000 });
  await sleep(jitter(politeDelayMs));
  if (debug) await snapshot(page, `detail-after-opening-${streetFrag}`);
}

// --- Downloader: handles download event, popup, or inline PDF ---
async function downloadCertificate({ page, context, key, debug }) {
  if (debug) await snapshot(page, `pre-download-${key}`);

  const downloadButton = page.getByText(/^\s*Download\s*$/i).first();

  // Prepare all possible completion signals *before* clicking
  const downloadPromise = page.waitForEvent('download', { timeout: 120_000 }).then(d => ({ kind: 'download', d })).catch(() => null);
  const popupPromise    = page.waitForEvent('popup',    { timeout: 120_000 }).then(p => ({ kind: 'popup', p })).catch(() => null);
  const responsePromise = page.waitForResponse(
    resp => (resp.headers()['content-type'] || '').toLowerCase().includes('application/pdf'),
    { timeout: 120_000 }
  ).then(r => ({ kind: 'response', r })).catch(() => null);

  await downloadButton.click({ delay: jitter(80, 160) });

  // Wait for whichever signal fires first (and poll up to 25s if needed)
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
      log.warning(`Stream read failed, retrying once: ${e.message}`);
      await page.waitForTimeout(2000);
      const stream = await signal.d.createReadStream();
      buffer = stream ? await streamToBuffer(stream) : buffer;
    }
  } else if (signal?.kind === 'response') {
    log.info('Inline PDF response detected; capturing body…');
    buffer = await signal.r.body();
  } else if (signal?.kind === 'popup') {
    log.info('Popup detected; trying to capture PDF from popup…');
    const p = signal.p;
    await p.waitForLoadState('domcontentloaded', { timeout: 60_000 }).catch(() => {});
    if (debug) await snapshot(p, `popup-opened-${key}`);

    const pdfResp = await p.waitForResponse(
      resp => (resp.headers()['content-type'] || '').toLowerCase().includes('application/pdf'),
      { timeout: 60_000 }
    ).catch(() => null);

    if (pdfResp) {
      buffer = await pdfResp.body();
    } else {
      const url = p.url();
      if (/\.pdf($|\?)/i.test(url)) {
        const resp = await context.request.get(url);
        if (resp && resp.ok()) buffer = await resp.body();
      }
    }
  } else {
    log.warning('No download/popup/response signal detected. Waiting an extra 25s just in case…');
  }

  // Belt-and-suspenders delay to avoid cutting off I/O
  await page.waitForTimeout(25000);

  return buffer || Buffer.alloc(0);
}

async function run() {
  await Actor.init();

  const input = (await Actor.getInput()) || {};
  const {
    loginUrl = 'https://app.ibhs.org/fh',
    addresses: rawAddresses = [],
    maxAddressesPerRun = 1,
    politeDelayMs = 800,
    debug = false,
    username: usernameFromInput,
    password: passwordFromInput,
  } = input;

  let addresses = [];
  if (Array.isArray(rawAddresses)) {
    addresses = rawAddresses;
  } else if (typeof rawAddresses === 'string') {
    addresses = rawAddresses.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
  }

  const username = usernameFromInput || process.env.IBHS_USERNAME;
  const password = passwordFromInput || process.env.IBHS_PASSWORD;

  if (!username || !password) throw new Error('Missing credentials.');
  if (!addresses.length) throw new Error('addresses[] is empty.');

  const processed = await loadProcessed();

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

  try {
    await ensureLoggedIn(page, { loginUrl, username, password, politeDelayMs, debug });

    let handled = 0;
    for (const raw of addresses) {
      if (handled >= maxAddressesPerRun) break;

      const addr = (raw || '').trim();
      const key = normalizeAddress(addr);
      if (!key) continue;
      if (processed[key]) { log.info(`Skip (already processed): ${addr}`); continue; }

      // SEARCH
      const searchField = await openSearch(page);
      if (!(await searchField.count())) {
        log.warning('Search field not found; reloading shell once.');
        await page.reload({ waitUntil: 'networkidle' });
        await sleep(jitter(politeDelayMs));
      }
      const box = (await openSearch(page)) || page.getByRole('textbox').first();
      await box.fill(addr);
      await sleep(jitter(250));
      await page.keyboard.press('Enter');

      await page.waitForLoadState('networkidle', { timeout: 60_000 });
      await sleep(jitter(politeDelayMs));
      if (debug) await snapshot(page, `search-results-${key}`);

      // OPEN FIRST RESULT
      try {
        await openFirstResult(page, addr, { politeDelayMs, debug });
      } catch {
        processed[key] = { status: 'no_results' };
        await saveProcessed(processed);
        handled++;
        continue;
      }

      // Go to Certificate(s) section
      let certControl = page.getByText(/^\s*Certificates?\s*$/i).first();
      if (!(await certControl.count())) {
        certControl = page.locator('[role="tab"], [role="link"], button, a')
          .filter({ hasText: /Certificates?/i })
          .first();
      }

      if (await certControl.count()) {
        await certControl.click();
        await page.waitForLoadState('networkidle', { timeout: 60_000 });
        await sleep(jitter(300));
        if (debug) await snapshot(page, `detail-after-certificate-${key}`);
      } else {
        processed[key] = { status: 'no_certificate' };
        await saveProcessed(processed);
        handled++;
        await page.goBack({ waitUntil: 'domcontentloaded' }).catch(() => {});
        continue;
      }

      // Find a Download control
      let hasDownload = await page.getByText(/^\s*Download\s*$/i).first().count();
      if (!hasDownload) {
        const downloadLike = page.locator('button, [role="button"], a').filter({ hasText: /download/i }).first();
        if (await downloadLike.count()) hasDownload = 1;
      }
      if (!hasDownload) {
        processed[key] = { status: 'no_download' };
        await saveProcessed(processed);
        handled++;
        await page.goBack({ waitUntil: 'domcontentloaded' }).catch(() => {});
        await page.waitForLoadState('networkidle', { timeout: 60_000 });
        continue;
      }

      // DOWNLOAD (handles event, popup, or inline)
      const buffer = await downloadCertificate({ page, context, key, debug });

      // Optional "Expires" scrape
      let expires = '';
      try {
        const expLabel = page.getByText(/expires/i).first();
        if (await expLabel.count()) {
          const near = await expLabel.evaluateHandle((el) => el.nextElementSibling?.textContent || '');
          expires = (await near.jsonValue())?.toString().trim() || '';
        }
      } catch {}

      const fileName = sanitizeFileName(
        `${key}${expires ? ` - Expires ${expires}` : ''}.pdf`.replace(/\s+/g, ' ')
      );

      // Save to Apify KV (canonical)
      await Actor.setValue(fileName, buffer, { contentType: 'application/pdf' });

      // Also mirror to ./downloads (local dev) and to Windows Downloads when present
      try {
        const repoDownloads = path.join(process.cwd(), 'downloads');
        await fs.mkdir(repoDownloads, { recursive: true });
        await fs.writeFile(path.join(repoDownloads, fileName), buffer);
        log.info(`Saved (repo): ${path.join(repoDownloads, fileName)}`);
      } catch (e) {
        log.warning(`Could not save to ./downloads: ${e.message}`);
      }
      try {
        const userDownloads = path.join(process.env.USERPROFILE || '', 'Downloads');
        if (userDownloads) {
          await fs.mkdir(userDownloads, { recursive: true });
          await fs.writeFile(path.join(userDownloads, fileName), buffer);
          log.info(`Saved (Windows Downloads): ${path.join(userDownloads, fileName)}`);
        }
      } catch (e) {
        // Cloud environment won't have USERPROFILE; that's fine.
      }

      // Upload to Google Drive folder (Cloud-native target)
      try {
        const uploaded = await uploadToGoogleDrive(buffer, fileName);
        if (uploaded?.id) {
          log.info(`Uploaded to Google Drive: ${uploaded.webViewLink || uploaded.id}`);
        } else {
          log.warning('Google Drive upload skipped or failed (no file id returned).');
        }
      } catch (e) {
        log.warning(`Google Drive upload error: ${e.message}`);
      }

      processed[key] = { status: 'downloaded', file: fileName, when: new Date().toISOString() };
      await saveProcessed(processed);

      // Brief pause
      await page.waitForTimeout(1000);

      // Back to results for next address
      await page.goBack({ waitUntil: 'domcontentloaded' }).catch(() => {});
      await page.waitForLoadState('networkidle', { timeout: 60_000 });
      if (debug) await snapshot(page, `back-to-results-${key}`);
      await sleep(jitter(politeDelayMs));

      handled++;
    }

    log.info(`Run complete. Addresses handled: ${handled}`);
  } finally {
    await browser.close().catch(() => {});
    await Actor.exit();
  }
}

Actor.main(run);
