const fs = require('fs');
const path = require('path');
const https = require('https');
const zlib = require('zlib');
const { execSync } = require('child_process');

// AODP servers
const AODP_SERVERS = [
  'west.albion-online-data.com',
  'europe.albion-online-data.com',
  'east.albion-online-data.com',
];

function fetchHistory(server, itemIds) {
  return new Promise((resolve, reject) => {
    const url = `https://${server}/api/v2/stats/history/${itemIds.join(',')}.json?time_scale=1`;
    const req = https.get(url, { headers: { 'Accept-Encoding': 'gzip' } }, (res) => {
      const rawChunks = [];
      res.on('data', (c) => rawChunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      res.on('end', () => {
        const buf = Buffer.concat(rawChunks);
        const decode = (b) => {
          const text = b.toString('utf8');
          if (res.statusCode === 429 || text.startsWith('Throttled')) {
            const err = new Error('Throttled');
            err.throttled = true;
            return reject(err);
          }
          try { resolve(JSON.parse(text)); }
          catch (e) { reject(e); }
        };
        if (res.headers['content-encoding'] === 'gzip') {
          zlib.gunzip(buf, (err, d) => err ? reject(err) : decode(d));
        } else {
          decode(buf);
        }
      });
      res.on('error', reject);
    });
    req.setTimeout(15000, () => req.destroy(new Error('Request timeout')));
    req.on('error', reject);
  });
}

// Retries forever every 6s on throttle or timeout
async function fetchWithRetry(server, itemIds) {
  let attempt = 0;
  while (true) {
    try {
      return await fetchHistory(server, itemIds);
    } catch (e) {
      if (e.throttled || e.message === 'Request timeout') {
        attempt++;
        console.warn(`  ⏳ Throttled/timeout (attempt ${attempt}) — retrying in 6s...`);
        await delay(6000);
        continue;
      }
      throw e;
    }
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Builds chunks keeping item list under the 4096-char URL limit
function buildChunks(itemIds, baseUrl) {
  const maxItemsLen = 4096 - baseUrl.length - '.json?time_scale=1'.length;
  const chunks = [];
  let current = [];
  let currentLen = 0;
  for (const id of itemIds) {
    const addLen = current.length === 0 ? id.length : id.length + 1;
    if (currentLen + addLen > maxItemsLen && current.length > 0) {
      chunks.push(current);
      current = [id];
      currentLen = id.length;
    } else {
      current.push(id);
      currentLen += addLen;
    }
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

async function capturePrices() {
  const runTimestamp = new Date().toISOString();
  const primaryServer = AODP_SERVERS[0];
  const startTime = Date.now();

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

  // Fetch 5 chunks concurrently, 5s between batches; chunk size respects 4096-char URL limit
  const pricesData = {};
  let genuineErrors = 0;
  const historyBase = `https://${primaryServer}/api/v2/stats/history/`;
  const chunks = buildChunks(itemIdArray, historyBase);
  const totalChunks = chunks.length;
  const concurrency = process.env.CONCURRENCY ? parseInt(process.env.CONCURRENCY, 10) : 5;

  for (let i = 0; i < chunks.length; i += concurrency) {
    const batch = chunks.slice(i, i + concurrency);
    const batchResults = await Promise.allSettled(batch.map((chunk) => fetchWithRetry(primaryServer, chunk)));
    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      if (result.status === 'rejected') {
        console.error(`[${i + j + 1}/${totalChunks}] Genuine error: ${result.reason.message}`);
        genuineErrors++;
        continue;
      }
      for (const row of result.value) {
        if (!pricesData[row.item_id]) pricesData[row.item_id] = [];
        pricesData[row.item_id].push(row);
      }
    }
    console.log(`✅ [${Math.min(i + concurrency, totalChunks)}/${totalChunks}] Fetched`);
  }

  if (Object.keys(pricesData).length === 0) {
    console.error('No price data available');
    process.exit(1);
  }

  console.log(`✅ Fetched history for ${Object.keys(pricesData).length} items`);

  const dataDir = path.join(__dirname, '..', 'data', 'prices');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  let writtenCount = 0;
  const changeLog = [];

  for (const [itemId, itemRows] of Object.entries(pricesData)) {
    const filePath = path.join(dataDir, `${itemId}.json`);
    let existing = [];

    if (fs.existsSync(filePath)) {
      try {
        const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
        // Accept new array format; discard legacy object format
        existing = Array.isArray(raw) ? raw : [];
      } catch (e) {
        console.warn(`Could not parse ${itemId}.json, starting fresh`);
      }
    }

    const entryMap = new Map(existing.map((e) => [`${e.city}|${e.quality}`, e]));
    let addedPoints = 0;
    for (const row of itemRows) {
      const key = `${row.city}|${row.quality}`;
      let cityEntry = entryMap.get(key);
      if (!cityEntry) {
        cityEntry = { city: row.city, quality: row.quality, data: [] };
        existing.push(cityEntry);
        entryMap.set(key, cityEntry);
      }
      const existingTs = new Set(cityEntry.data.map((d) => d.timestamp));
      for (const dp of (row.data || [])) {
        if (!existingTs.has(dp.timestamp)) {
          cityEntry.data.push(dp);
          existingTs.add(dp.timestamp);
          addedPoints++;
        }
      }
    }

    if (addedPoints > 0) {
      try {
        fs.writeFileSync(filePath, JSON.stringify(existing));
        writtenCount++;
        changeLog.push({ itemId, newPoints: addedPoints });
      } catch (e) {
        console.warn(`Failed to write ${itemId}.json:`, e.message);
      }
    }
  }

  console.log(`✅ Written ${writtenCount} item price files`);

  // Write changelog
  {
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
    changeLogData.changes.push({ timestamp: runTimestamp, itemsWritten: writtenCount, changes: changeLog });

    if (changeLogData.changes.length > 200) {
      changeLogData.changes = changeLogData.changes.slice(-200);
    }

    try {
      fs.writeFileSync(changeLogPath, JSON.stringify(changeLogData, null, 2));
      console.log(`📝 Logged ${changeLog.length} item changes`);
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

  console.log(`✅ Complete! Updated ${writtenCount} items, ${changeLog.length} price changes — ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
  process.exit(genuineErrors > 0 ? 1 : 0);
}

capturePrices().catch((e) => {
  console.error('Fatal error:', e);
  process.exit(1);
});
