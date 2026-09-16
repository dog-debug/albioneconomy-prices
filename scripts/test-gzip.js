const https = require('https');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const SERVER_URL = process.env.SERVER_URL || 'https://west.albion-online-data.com';

const allIds = [...new Set(
  JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'lib', 'albion_data', 'items.json'), 'utf8'))
    .map((i) => i.UniqueName).filter(Boolean),
)];

const base = `${SERVER_URL}/api/v2/stats/history/`;
const suffix = '.json?time_scale=1';
const maxItemsLen = 4096 - base.length - suffix.length;
let list = '';
for (const id of allIds) {
  const add = list ? `,${id}` : id;
  if (list.length + add.length > maxItemsLen) break;
  list += add;
}

const url = `${base}${list}${suffix}`;

console.log(`URL length : ${url.length} / 4096 chars`);
console.log(`Items      : ${list.split(',').length}`);
console.log('Sending    : Accept-Encoding: gzip\n');

const req = https.get(url, { headers: { 'Accept-Encoding': 'gzip' } }, (res) => {
  const encoding = res.headers['content-encoding'];
  const isGzip = encoding === 'gzip';

  console.log(`Status           : ${res.statusCode}`);
  console.log(`content-encoding : ${encoding || '(none)'}`);
  console.log(`Gzip active      : ${isGzip ? '✅ YES — decompressing...' : '❌ NO — server did not compress'}\n`);

  // Collect raw compressed bytes from the wire first, then decompress
  const rawChunks = [];
  res.on('data', (c) => rawChunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
  res.on('end', () => {
    const compressedBuffer = Buffer.concat(rawChunks);
    const compressedBytes = compressedBuffer.length;

    const finish = (decompressed) => {
      let data;
      try {
        data = JSON.parse(decompressed.toString('utf8'));
      } catch (e) {
        console.error('Parse failed:', e.message);
        console.error('Raw (first 200):', decompressed.toString('utf8').slice(0, 200));
        process.exit(1);
      }

      const rawBytes = decompressed.length;
      const ratio = isGzip ? ((1 - compressedBytes / rawBytes) * 100).toFixed(1) : 'N/A';

      console.log(`Entries returned : ${data.length}`);
      console.log(`Compressed size  : ${isGzip ? compressedBytes.toLocaleString() + ' bytes' : 'N/A'}`);
      console.log(`Uncompressed size: ${rawBytes.toLocaleString()} bytes`);
      console.log(`Compression ratio: ${ratio}${isGzip ? '% smaller' : ''}\n`);
      console.log('Sample entry:\n' + JSON.stringify(data[0], null, 2).slice(0, 500));
      console.log('\n✅ Test complete');
      process.exit(0);
    };

    if (isGzip) {
      zlib.gunzip(compressedBuffer, (err, decompressed) => {
        if (err) { console.error('Gunzip error:', err.message); process.exit(1); }
        finish(decompressed);
      });
    } else {
      finish(compressedBuffer);
    }
  });
  res.on('error', (e) => { console.error('Stream error:', e.message); process.exit(1); });
});

req.setTimeout(15000, () => { req.destroy(new Error('Request timed out')); });
req.on('error', (e) => { console.error('Request error:', e.message); process.exit(1); });
