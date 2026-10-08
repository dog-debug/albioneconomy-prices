const fs = require('fs');
const path = require('path');
const https = require('https');
const zlib = require('zlib');
const { execSync } = require('child_process');

const SERVER_URL = process.env.SERVER_URL;
const SERVER_NAME = process.env.SERVER_NAME;

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function isRetryableError(error) {
  if (!error) return false;
  const message = String(error.message || error).toLowerCase();
  const code = error.code ? String(error.code).toLowerCase() : '';
  const httpStatus = Number(error.httpStatus);

  return Boolean(error.throttled)
    || (Number.isInteger(httpStatus) && httpStatus >= 500)
    || message.includes('throttl')
    || message.includes('timeout')
    || message.includes('econnreset')
    || message.includes('econnaborted')
    || message.includes('socket hang up')
    || message.includes('temporar')
    || message.includes('network')
    || message.includes('parse error')
    || message.includes('unexpected end of')
    || message.includes('unexpected eof')
    || code === 'econnreset'
    || code === 'econnaborted'
    || code === 'enotfound'
    || code === 'eai_again'
    || code === 'timedout';
}

if (!SERVER_URL || !SERVER_NAME) {
  console.error('Missing SERVER_URL or SERVER_NAME env vars');
  process.exit(1);
}

function fetchHistory(itemIds) {
  return new Promise((resolve, reject) => {
    const url = `${SERVER_URL}/api/v2/stats/history/${itemIds.join(',')}.json?time_scale=1`;
    const req = https.get(url, { headers: { 'Accept-Encoding': 'gzip' } }, (res) => {
      const rawChunks = [];
      res.on('data', (c) => rawChunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      res.on('end', () => {
        const buf = Buffer.concat(rawChunks);
        const decode = (b) => {
          const text = b.toString('utf8');
          if (res.statusCode === 429 || res.statusCode >= 500 || text.startsWith('Throttled')) {
            const err = new Error(`HTTP ${res.statusCode} response`);
            err.httpStatus = res.statusCode;
            err.throttled = res.statusCode === 429 || text.startsWith('Throttled');
            return reject(err);
          }
          try { resolve(JSON.parse(text)); }
          catch (e) { reject(new Error(`Parse error: ${e.message}`)); }
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

function getRetrySettings() {
  return {
    maxRetries: parsePositiveInt(process.env.MAX_RETRIES, 3),
    maxSplitDepth: parsePositiveInt(process.env.MAX_SPLIT_DEPTH, 4),
    minSplitSize: parsePositiveInt(process.env.MIN_SPLIT_SIZE, 16),
    baseDelayMs: parsePositiveInt(process.env.RETRY_BASE_DELAY_MS, 1500),
    maxDelayMs: parsePositiveInt(process.env.RETRY_MAX_DELAY_MS, 8000),
  };
}

async function fetchWithRetry(itemIds, maxRetriesOverride) {
  const retrySettings = getRetrySettings();
  const maxRetries = Number.isInteger(maxRetriesOverride)
    ? maxRetriesOverride
    : retrySettings.maxRetries;
  let attempt = 0;
  let lastError;

  while (attempt <= maxRetries) {
    try {
      return await fetchHistory(itemIds);
    } catch (e) {
      lastError = e;
      if (!isRetryableError(e) || attempt >= maxRetries) {
        throw e;
      }

      attempt++;
      const backoffMs = Math.min(retrySettings.baseDelayMs * (2 ** (attempt - 1)), retrySettings.maxDelayMs);
      console.warn(`  ⏳ Retryable request failure (attempt ${attempt}/${maxRetries}) — retrying in ${backoffMs}ms...`);
      await delay(backoffMs);
    }
  }

  throw lastError;
}

function delay(ms) { return new Promise((r) => setTimeout(r, ms)); }

function splitChunk(itemIds) {
  if (itemIds.length <= 1) return [itemIds];
  const midpoint = Math.ceil(itemIds.length / 2);
  return [itemIds.slice(0, midpoint), itemIds.slice(midpoint)];
}

async function fetchChunkWithFallback(itemIds, depth = 0) {
  const retrySettings = getRetrySettings();

  try {
    return await fetchWithRetry(itemIds);
  } catch (error) {
    if (itemIds.length <= retrySettings.minSplitSize || depth >= retrySettings.maxSplitDepth) {
      console.warn(`  ⚠️ Skipping unrecoverable item group (${itemIds.length} items) after ${depth} split(s): ${error.message}`);
      return [];
    }

    const halves = splitChunk(itemIds);
    const results = await Promise.allSettled(
      halves.map((half) => fetchChunkWithFallback(half, depth + 1)),
    );

    const recovered = [];
    for (const result of results) {
      if (result.status === 'fulfilled') {
        recovered.push(...(Array.isArray(result.value) ? result.value : []));
      } else {
        console.warn(`  ⚠️ Failed to recover split chunk at depth ${depth}: ${result.reason?.message || result.reason}`);
      }
    }

    return recovered;
  }
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

async function main() {
  const startTime = Date.now();
  const runTimestamp = new Date().toISOString();
  console.log(`[${SERVER_NAME}] Starting history capture at ${runTimestamp}`);

  const itemsPath = path.join(__dirname, '..', 'lib', 'albion_data', 'items.json');
  if (!fs.existsSync(itemsPath)) {
    console.error('items.json not found:', itemsPath);
    process.exit(1);
  }

  const itemIds = [...new Set(
    JSON.parse(fs.readFileSync(itemsPath, 'utf8')).map((i) => i.UniqueName).filter(Boolean),
  )];
  console.log(`Loaded ${itemIds.length} items`);

  // TEMP: local test path — revert to 'albion prices', 'daily', SERVER_NAME for production
  const outDir = path.join(__dirname, '..', 'data', SERVER_NAME);
  fs.mkdirSync(outDir, { recursive: true });

  let writtenCount = 0;
  const changeEntries = [];
  let genuineErrors = 0;

  const historyBase = `${SERVER_URL}/api/v2/stats/history/`;
  const chunks = buildChunks(itemIds, historyBase);
  const totalChunks = chunks.length;
  const concurrency = parsePositiveInt(process.env.CONCURRENCY, 5);

  for (let i = 0; i < chunks.length; i += concurrency) {
    const batch = chunks.slice(i, i + concurrency);
    const batchResults = await Promise.allSettled(batch.map((chunk) => fetchChunkWithFallback(chunk)));

    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      const chunkNum = i + j + 1;
      if (result.status === 'rejected') {
        console.error(`[${chunkNum}/${totalChunks}] Genuine error: ${result.reason.message}`);
        genuineErrors++;
        continue;
      }

      const grouped = {};
      for (const row of result.value || []) {
        if (!grouped[row.item_id]) grouped[row.item_id] = [];
        grouped[row.item_id].push(row);
      }

      for (const [itemId, itemRows] of Object.entries(grouped)) {
        const filePath = path.join(outDir, `${itemId}.json`);
        let existing = [];
        if (fs.existsSync(filePath)) {
          try { existing = JSON.parse(fs.readFileSync(filePath, 'utf8')); }
          catch { existing = []; }
        }
        if (!Array.isArray(existing)) existing = [];

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
          fs.writeFileSync(filePath, JSON.stringify(existing));
          writtenCount++;
          changeEntries.push({ itemId, newPoints: addedPoints });
        }
      }
    }

    console.log(`[${Math.min(i + concurrency, totalChunks)}/${totalChunks}] Done`);
  }

  const changeLogPath = path.join(outDir, 'CHANGELOG.json');
  let log = { lastUpdate: runTimestamp, runs: [] };
  if (fs.existsSync(changeLogPath)) {
    try { log = JSON.parse(fs.readFileSync(changeLogPath, 'utf8')); }
    catch { log = { lastUpdate: runTimestamp, runs: [] }; }
  }
  if (!Array.isArray(log.runs)) log.runs = [];
  log.lastUpdate = runTimestamp;
  log.runs.push({ timestamp: runTimestamp, itemsWritten: writtenCount, changes: changeEntries });
  if (log.runs.length > 200) log.runs = log.runs.slice(-200);
  fs.writeFileSync(changeLogPath, JSON.stringify(log, null, 2));

  try {
    execSync('git config user.name "github-actions"', { cwd: process.cwd() });
    execSync('git config user.email "github-actions@github.com"', { cwd: process.cwd() });
    execSync(`git add 'albion prices/daily/${SERVER_NAME}/'`, { cwd: process.cwd() });
    try {
      execSync(`git commit -m "[${SERVER_NAME}] history: ${writtenCount} items @ ${runTimestamp}"`, { cwd: process.cwd() });
      execSync('git push', { cwd: process.cwd() });
      console.log('Pushed successfully');
    } catch {
      console.log('No changes to commit');
    }
  } catch (e) {
    console.error('Git error:', e.message);
  }

  if (genuineErrors > 0) {
    console.warn(`Done with best effort. Written ${writtenCount} items after skipping ${genuineErrors} unrecoverable chunks.`);
  } else {
    console.log(`Done. Written ${writtenCount} items — ${((Date.now() - startTime) / 1000).toFixed(1)}s`);
  }
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
