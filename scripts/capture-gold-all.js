const fs = require('fs');
const path = require('path');
const https = require('https');
const zlib = require('zlib');
const { execSync } = require('child_process');

const SERVERS = [
  { name: 'west', url: 'https://west.albion-online-data.com' },
  { name: 'east', url: 'https://east.albion-online-data.com' },
  { name: 'eu', url: 'https://europe.albion-online-data.com' }
];

function fetchGold(serverUrl) {
  return new Promise((resolve, reject) => {
    const url = `${serverUrl}/api/v2/stats/gold.json?count=1000`;
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

async function captureServer(server) {
  const { name, url } = server;
  const runTimestamp = new Date().toISOString();
  console.log(`[${name}] Starting gold capture at ${runTimestamp}`);

  const outDir = path.join(__dirname, '..', 'albion prices', 'daily', name);
  fs.mkdirSync(outDir, { recursive: true });

  let goldData;
  let goldAttempt = 0;
  while (true) {
    try {
      goldData = await fetchGold(url);
      break;
    } catch (e) {
      if (e.throttled || e.message === 'Request timeout') {
        goldAttempt++;
        console.warn(`  ⏳ [${name}] Throttled/timeout (attempt ${goldAttempt}) — retrying in 6s...`);
        await new Promise((r) => setTimeout(r, 6000));
        continue;
      }
      console.error(`  ❌ [${name}] Failed to fetch gold:`, e.message);
      throw e;
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
  console.log(`  ✅ [${name}] Added ${newEntries.length} new gold entries (${existing.length} total)`);

  return { name, newEntries, totalCount: existing.length };
}

async function main() {
  console.log('🕐 Starting gold capture for all servers');
  
  const startTime = Date.now();
  const results = [];
  
  // Fetch all 3 servers in parallel for speed
  const promises = SERVERS.map(s => captureServer(s));
  const settled = await Promise.allSettled(promises);
  
  for (const result of settled) {
    if (result.status === 'fulfilled') {
      results.push(result.value);
    } else {
      console.error('❌ Error:', result.reason?.message || result.reason);
      process.exit(1);
    }
  }

  // Commit all changes together
  try {
    execSync('git config user.name "github-actions"', { cwd: process.cwd() });
    execSync('git config user.email "github-actions@github.com"', { cwd: process.cwd() });
    
    for (const { name } of SERVERS) {
      execSync(`git add 'albion prices/daily/${name}/gold.json'`, { cwd: process.cwd() });
    }
    
    const runTimestamp = new Date().toISOString();
    const summary = results.map(r => `${r.name}(+${r.newEntries.length})`).join(', ');
    execSync(`git commit -m "gold: ${summary} @ ${runTimestamp}"`, { cwd: process.cwd() });
    execSync('git push', { cwd: process.cwd() });
    console.log('✅ Pushed successfully');
  } catch (e) {
    if (e.message.includes('no changes added')) {
      console.log('ℹ️  No changes to commit');
    } else {
      console.error('Git error:', e.message);
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`✅ All gold captures completed in ${elapsed}s`);
  process.exit(0);
}

main().catch((e) => { console.error(e); process.exit(1); });
