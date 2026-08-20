const fs = require('fs');
const path = require('path');
const https = require('https');
const { execSync } = require('child_process');

// AODP servers
const AODP_SERVERS = [
  'west.albion-online-data.com',
  'europe.albion-online-data.com',
  'east.albion-online-data.com',
];

// City abbreviations
const CITY_ABBREVIATIONS = {
  'Caerleon': 'caerleon',
  'Bridgewatch': 'bridgewatch',
  'Martlock': 'martlock',
  'Thetford': 'thetford',
  'Fort Sterling': 'forsterling',
  'Lymhurst': 'lymhurst',
  'Brecilien': 'brecilien',
  'Black Market': 'blackmarket',
};

async function fetchAodpPrices(server, itemIds) {
  return new Promise((resolve, reject) => {
    const itemList = itemIds.join(',');
    const url = `https://${server}/api/v2/stats/prices/${itemList}.json`;
    https.get(url, { timeout: 10000 }, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        try {
          // Check if we got a valid JSON response (not HTML error page)
          if (res.statusCode !== 200) {
            reject(new Error(`HTTP ${res.statusCode}`));
            return;
          }
          resolve(JSON.parse(data));
        } catch (e) {
          reject(e);
        }
      });
    }).on('error', reject);
  });
}

async function fetchWithRetry(server, itemIds, maxRetries = 3) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fetchAodpPrices(server, itemIds);
    } catch (e) {
      const isLastAttempt = attempt === maxRetries;
      const backoffMs = Math.pow(2, attempt - 1) * 1000; // 1s, 2s, 4s
      
      if (isLastAttempt) {
        console.warn(`❌ All ${maxRetries} retries failed for ${itemIds.length} items`);
        throw e;
      }
      
      console.warn(`⚠️  Retry ${attempt}/${maxRetries} for ${itemIds.length} items (${e.message}) - waiting ${backoffMs}ms`);
      await delay(backoffMs);
    }
  }
}

function formatPrice(price) {
  // DEPRECATED: Keeping for reference but no longer used
  // App now uses raw numbers directly for calculations
  if (price >= 1000000) {
    const m = price / 1000000;
    const formatted = m.toFixed(1);
    return formatted.endsWith('.0') ? formatted.slice(0, -2) : formatted + 'M';
  }
  if (price >= 1000) {
    const k = price / 1000;
    const formatted = k.toFixed(1);
    return formatted.endsWith('.0') ? formatted.slice(0, -2) : formatted + 'K';
  }
  return String(Math.round(price));
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function validatePrice(price, fieldName, itemId, city) {
  // Return null if price is falsy (null, undefined, 0)
  if (!price) return null;
  
  // Check if price is a valid number
  if (!Number.isFinite(price)) {
    console.warn(`⚠️  Invalid ${fieldName} for ${itemId} in ${city}: ${price} (not a valid number)`);
    return null;
  }
  
  // Reject negative prices
  if (price < 0) {
    console.warn(`⚠️  Negative ${fieldName} for ${itemId} in ${city}: ${price} (rejected)`);
    return null;
  }
  
  return price;
}

async function capturePrices() {
  const runTimestamp = new Date().toISOString();
  const primaryServer = AODP_SERVERS[0];

  console.log(`🕐 Starting price capture at ${runTimestamp}`);

  // Load items from items.json
  const itemsJsonPath = path.join(__dirname, '..', 'lib', 'albion_data', 'items.json');
  if (!fs.existsSync(itemsJsonPath)) {
    console.error('❌ items.json not found at:', itemsJsonPath);
    process.exit(1);
  }

  let itemsData;
  try {
    itemsData = JSON.parse(fs.readFileSync(itemsJsonPath, 'utf8'));
  } catch (e) {
    console.error('Failed to parse items.json:', e.message);
    process.exit(1);
  }

  // Extract all unique item IDs
  const allItemIds = new Set();
  for (const item of itemsData) {
    if (item.UniqueName) {
      allItemIds.add(item.UniqueName);
    }
  }

  const itemIdArray = Array.from(allItemIds);
  console.log(`📦 Loaded ${itemIdArray.length} unique items from items.json`);

  // Fetch prices in chunks (AODP has 250 item limit per request)
  const pricesData = {};
  const chunkSize = 250;
  const totalChunks = Math.ceil(itemIdArray.length / chunkSize);

  for (let i = 0; i < itemIdArray.length; i += chunkSize) {
    const chunkNum = Math.floor(i / chunkSize) + 1;
    const chunk = itemIdArray.slice(i, i + chunkSize);
    
    try {
      const prices = await fetchWithRetry(primaryServer, chunk);
      for (const priceRow of prices) {
        const itemId = priceRow.item_id;
        if (!pricesData[itemId]) {
          pricesData[itemId] = [];
        }
        pricesData[itemId].push(priceRow);
      }
      console.log(`✅ [${chunkNum}/${totalChunks}] Fetched ${chunk.length} items`);
      await delay(1000); // 1 req/sec = 60 req/min (well under 180 limit)
    } catch (e) {
      console.warn(`[${chunkNum}/${totalChunks}] Failed:`, e.message);
      await delay(2000);
    }
  }

  if (Object.keys(pricesData).length === 0) {
    console.error('No price data available');
    process.exit(1);
  }

  console.log(`✅ Fetched prices for ${Object.keys(pricesData).length} items`);

  // Write to local data/prices/ directory
  const dataDir = path.join(__dirname, '..', 'data', 'prices');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  let writtenCount = 0;
  const changeLog = [];

  for (const [itemId, priceRows] of Object.entries(pricesData)) {
    const filePath = path.join(dataDir, `${itemId}.json`);
    let itemData = { itemId, priceHistory: [] };

    // Load existing data
    if (fs.existsSync(filePath)) {
      try {
        itemData = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        if (!Array.isArray(itemData.priceHistory)) {
          itemData.priceHistory = [];
        }
      } catch (e) {
        console.warn(`Could not parse ${itemId}.json, starting fresh`);
      }
    }

    let hasNewData = false;

    for (const row of priceRows) {
      // Validate prices before using them
      const sellPrice = validatePrice(row.sell_price_min, 'sellPrice', itemId, row.city);
      const buyPrice = validatePrice(row.buy_price_max, 'buyPrice', itemId, row.city);

      // Skip if both prices are invalid/null
      if (sellPrice === null && buyPrice === null) {
        continue;
      }

      hasNewData = true;
      const city = CITY_ABBREVIATIONS[row.city] || row.city.toLowerCase();
      const quality = row.quality;

      const newEntry = {
        timestamp: runTimestamp,
        city,
        quality,
        server: primaryServer,
        sellPrice: sellPrice,
        buyPrice: buyPrice,
      };

      const existingEntry = itemData.priceHistory.find(
        h => h.timestamp === runTimestamp && h.city === city && h.quality === quality && h.server === primaryServer
      );

      if (!existingEntry) {
        const lastEntry = [...itemData.priceHistory]
          .reverse()
          .find(h => h.city === city && h.quality === quality && h.server === primaryServer);

        if (lastEntry && (lastEntry.sellPrice !== newEntry.sellPrice || lastEntry.buyPrice !== newEntry.buyPrice)) {
          changeLog.push({
            timestamp: runTimestamp,
            itemId,
            city,
            quality,
            oldSellPrice: lastEntry.sellPrice,
            newSellPrice: newEntry.sellPrice,
            oldBuyPrice: lastEntry.buyPrice,
            newBuyPrice: newEntry.buyPrice,
          });
        }

        itemData.priceHistory.push(newEntry);

        // Update latest prices (raw numbers only)
        if (!itemData.latest) itemData.latest = {};
        if (!itemData.latest[city]) itemData.latest[city] = {};
        
        itemData.latest[city][quality] = {
          timestamp: runTimestamp,
          sellPrice: newEntry.sellPrice,
          buyPrice: newEntry.buyPrice,
        };
      }
    }

    if (hasNewData) {
      try {
        fs.writeFileSync(filePath, JSON.stringify(itemData, null, 2));
        writtenCount++;
      } catch (e) {
        console.warn(`Failed to write ${itemId}.json:`, e.message);
      }
    }
  }

  console.log(`✅ Written ${writtenCount} item price files`);

  // Write changelog
  if (changeLog.length > 0) {
    const changeLogPath = path.join(dataDir, 'CHANGELOG.json');
    let changeLogData = { lastUpdate: runTimestamp, changes: [] };

    if (fs.existsSync(changeLogPath)) {
      try {
        changeLogData = JSON.parse(fs.readFileSync(changeLogPath, 'utf8'));
        if (!Array.isArray(changeLogData.changes)) {
          changeLogData.changes = [];
        }
      } catch (e) {
        console.warn('Could not parse CHANGELOG.json, starting fresh');
      }
    }

    changeLogData.lastUpdate = runTimestamp;
    changeLogData.changes.push(...changeLog);

    if (changeLogData.changes.length > 1000) {
      changeLogData.changes = changeLogData.changes.slice(-1000);
    }

    try {
      fs.writeFileSync(changeLogPath, JSON.stringify(changeLogData, null, 2));
      console.log(`📝 Logged ${changeLog.length} price changes`);
    } catch (e) {
      console.warn('Failed to write CHANGELOG.json:', e.message);
    }
  }

  // Commit and push to THIS repo
  try {
    console.log('📤 Committing to GitHub...');
    execSync('git config user.name "GitHub Actions"', { cwd: process.cwd() });
    execSync('git config user.email "actions@github.com"', { cwd: process.cwd() });
    execSync('git add data/prices/', { cwd: process.cwd() });
    
    try {
      const commitMsg = `Price snapshot: ${writtenCount} items updated, ${changeLog.length} changes logged`;
      execSync(`git commit -m "${commitMsg}"`, { cwd: process.cwd() });
      execSync('git push', { cwd: process.cwd() });
      console.log('✅ Successfully pushed prices!');
    } catch (commitErr) {
      console.log('ℹ️  No changes to commit');
    }
  } catch (e) {
    console.error('❌ Git error:', e.message);
  }

  console.log(`✅ Complete! Updated ${writtenCount} items, ${changeLog.length} price changes`);
  process.exit(0);
}

capturePrices().catch((e) => {
  console.error('Fatal error:', e);
  process.exit(1);
});
