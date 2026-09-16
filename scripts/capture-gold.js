const fs = require('fs');
const path = require('path');
const https = require('https');
const zlib = require('zlib');
const { execSync } = require('child_process');

const SERVER_URL = process.env.SERVER_URL;
const SERVER_NAME = process.env.SERVER_NAME;

if (!SERVER_URL || !SERVER_NAME) {
  console.error('Missing SERVER_URL or SERVER_NAME env vars');
  process.exit(1);
}

function fetchGold() {
  return new Promise((resolve, reject) => {
    const url = `${SERVER_URL}/api/v2/stats/gold.json?count=1000`;
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
    req.setTimeout(10000, () => req.destroy(new Error('Request timeout')));
    req.on('error', reject);
  });
}

async function main() {
  const runTimestamp = new Date().toISOString();
  console.log(`[${SERVER_NAME}] Starting gold capture at ${runTimestamp}`);

  const outDir = path.join(__dirname, '..', 'albion prices', 'daily', SERVER_NAME);
  fs.mkdirSync(outDir, { recursive: true });

  let goldData;
  let goldAttempt = 0;
  while (true) {
    try {
      goldData = await fetchGold();
      break;
    } catch (e) {
      if (e.throttled || e.message === 'Request timeout') {
        goldAttempt++;
        console.warn(`Throttled/timeout (attempt ${goldAttempt}) — retrying in 6s...`);
        await new Promise((r) => setTimeout(r, 6000));
        continue;
      }
      console.error('Failed to fetch gold:', e.message);
      process.exit(1);
    }
  }

  const goldPath = path.join(outDir, 'gold.json');
  let existing = [];
  if (fs.existsSync(goldPath)) {
    try { existing = JSON.parse(fs.readFileSync(goldPath, 'utf8')); }
    catch { existing = []; }
  }
  if (!Array.isArray(existing)) existing = [];

  const existingTs = new Set(existing.map((e) => e.timestamp));
  const incoming = Array.isArray(goldData) ? goldData : [goldData];
  const newEntries = incoming.filter((e) => e && e.timestamp && !existingTs.has(e.timestamp));
  existing.push(...newEntries);
  fs.writeFileSync(goldPath, JSON.stringify(existing, null, 2));
  console.log(`Added ${newEntries.length} new gold entries (${existing.length} total)`);

  try {
    execSync('git config user.name "github-actions"', { cwd: process.cwd() });
    execSync('git config user.email "github-actions@github.com"', { cwd: process.cwd() });
    execSync(`git add 'albion prices/daily/${SERVER_NAME}/gold.json'`, { cwd: process.cwd() });
    try {
      execSync(`git commit -m "[${SERVER_NAME}] gold: +${newEntries.length} entries @ ${runTimestamp}"`, { cwd: process.cwd() });
      execSync('git push', { cwd: process.cwd() });
      console.log('Pushed successfully');
    } catch {
      console.log('No changes to commit');
    }
  } catch (e) {
    console.error('Git error:', e.message);
  }

  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
