// test-local.js - FIXED VERSION
import { chromium } from 'playwright';

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function test() {
  console.log('🚀 Starting test with visible browser...');
  
  const browser = await chromium.launch({
    headless: false,
    slowMo: 1000
  });

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119 Safari/537.36'
  });

  const page = await context.newPage();

  try {
    // Login
    console.log('🔐 Navigating to login page...');
    await page.goto('https://app.ibhs.org/fh');
    await sleep(3000);

    console.log('📝 Filling credentials...');
    await page.fill('input[type="email"]', 'Knockoutinspections@gmail.com');
    await sleep(500);
    await page.fill('input[type="password"]', 'Sunni1122!');
    await sleep(500);
    
    console.log('🔑 Clicking login...');
    await page.click('button:has-text("Sign in")');
    await sleep(5000);

    // Click New Evaluation
    console.log('📋 Clicking "New Evaluation"...');
    await page.getByText(/New Evaluation/i).first().click();
    await sleep(3000);

    // Click Redesignation
    console.log('🔄 Clicking "Redesignation"...');
    await page.getByText(/Redesignation/i).first().click();
    await sleep(3000);

    // Search for address
    console.log('🔍 Finding search field...');
    const searchField = page.locator('input[placeholder*="Type to search"]').nth(1);
    
    if (await searchField.count() > 0) {
      console.log('✅ Found search field!');
      await searchField.click();
      await sleep(500);
      
      console.log('⌨️ Typing "513 Malaga"...');
      await searchField.type('513 Malaga', { delay: 150 });
      
      // Wait for dropdown to appear and populate
      console.log('⏳ Waiting for dropdown results...');
      await sleep(3000); // Give time for autocomplete to filter
      
      // Look for dropdown suggestions - try multiple selectors
      console.log('🔍 Looking for dropdown suggestions...');
      
      const dropdownSelectors = [
        '.e-popup.e-popup-open .e-list-item',  // Common autocomplete dropdown
        '.e-dropdownbase .e-list-item',
        '[role="listbox"] [role="option"]',
        '.e-autocomplete .e-list-item',
        'ul.e-list-parent li',
        '.bp5-menu-item',
        '[class*="suggestion"]',
        '[class*="dropdown"] li',
        '[class*="autocomplete"] li'
      ];
      
      let foundDropdown = false;
      
      for (const selector of dropdownSelectors) {
        const items = await page.locator(selector).all();
        
        if (items.length > 0) {
          console.log(`✅ Found ${items.length} dropdown items with selector: ${selector}`);
          
          // Log the text of each item
          for (let i = 0; i < items.length; i++) {
            const text = await items[i].textContent();
            console.log(`   ${i + 1}. ${text}`);
          }
          
          // Click the first item
          console.log('🖱️ Clicking first dropdown result...');
          await items[0].click();
          foundDropdown = true;
          break;
        }
      }
      
      if (!foundDropdown) {
        console.log('⚠️ No dropdown found, trying keyboard navigation...');
        // Use arrow down and enter as fallback
        await page.keyboard.press('ArrowDown');
        await sleep(500);
        await page.keyboard.press('Enter');
      }
      
      await sleep(5000);
      
      // Check if popup changed
      console.log('📄 Getting popup content after selection...');
      const popupText = await page.locator('[role="dialog"]').textContent().catch(() => 'NO DIALOG');
      console.log('Popup text:');
      console.log(popupText.substring(0, 500));
      
      // Take screenshot
      await page.screenshot({ path: 'debug-after-selection.png', fullPage: true });
      console.log('📸 Screenshot saved!');
      
    } else {
      console.error('❌ Search field not found!');
    }

    console.log('\n✅ Test complete!');
    console.log('⏸️ Browser will stay open for 60 seconds...');
    await sleep(60000);

  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error(error.stack);
    await page.screenshot({ path: 'debug-error.png', fullPage: true });
  } finally {
    console.log('🛑 Closing browser...');
    await browser.close();
  }
}

test();