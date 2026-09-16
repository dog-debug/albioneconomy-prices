#!/usr/bin/env node
const { spawn } = require('child_process');
const path = require('path');

const startTime = Date.now();

// Detailed logging wrapper
function log(level, msg) {
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`[${elapsed}s] [${level}] ${msg}`);
}

function runScript(script, env = {}) {
  return new Promise((resolve, reject) => {
    log('INFO', `Starting ${script}...`);
    // For combined runs, reduce concurrency to 2 to avoid overwhelming rate limits
    const envWithConcurrency = { ...env, CONCURRENCY: '2' };
    const proc = spawn('node', [path.join(__dirname, script)], {
      stdio: 'inherit',
      env: { ...process.env, ...envWithConcurrency }
    });
    
    proc.on('exit', (code) => {
      if (code === 0) {
        log('OK', `${script} finished successfully`);
        resolve();
      } else {
        log('ERROR', `${script} exited with code ${code}`);
        reject(new Error(`${script} failed with exit code ${code}`));
      }
    });
    
    proc.on('error', (e) => {
      log('ERROR', `${script} spawn error: ${e.message}`);
      reject(e);
    });
  });
}

async function main() {
  log('START', 'Combined capture workflow starting');
  log('INFO', 'Running all 4 operations sequentially');
  
  try {
    log('INFO', '=== [1/4] Capture History (West) ===');
    await runScript('capture-history-prices.js', {
      SERVER_URL: 'https://west.albion-online-data.com',
      SERVER_NAME: 'west'
    });
    
    log('INFO', '=== [2/4] Capture History (East) ===');
    await runScript('capture-history-prices.js', {
      SERVER_URL: 'https://east.albion-online-data.com',
      SERVER_NAME: 'east'
    });
    
    log('INFO', '=== [3/4] Capture History (EU) ===');
    await runScript('capture-history-prices.js', {
      SERVER_URL: 'https://europe.albion-online-data.com',
      SERVER_NAME: 'eu'
    });
    
    log('INFO', '=== [4/4] Capture Gold (All Servers) ===');
    await runScript('capture-gold-all.js');
    
    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    log('OK', `All 4 capture operations completed in ${totalTime}s`);
    log('INFO', `Ran with concurrency=2 per script to respect API rate limits`);
    
    process.exit(0);
  } catch (e) {
    log('FATAL', `Combined workflow failed: ${e.message}`);
    process.exit(1);
  }
}

main();
