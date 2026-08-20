const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

/**
 * MANUAL CLEANUP SCRIPT
 * Removes all sellPriceFormatted and buyPriceFormatted fields from existing price data
 * Keep only raw numbers for calculations
 */

async function cleanupPrices() {
  const dataDir = path.join(__dirname, '..', 'data', 'prices');
  
  if (!fs.existsSync(dataDir)) {
    console.error('❌ data/prices directory not found');
    process.exit(1);
  }

  const files = fs.readdirSync(dataDir);
  let cleanedCount = 0;
  let totalEntries = 0;

  console.log(`🧹 Starting cleanup of ${files.length} price files...`);

  for (const file of files) {
    if (!file.endsWith('.json') || file === 'CHANGELOG.json') continue;

    const filePath = path.join(dataDir, file);
    
    try {
      let data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      let modified = false;

      // Clean priceHistory array
      if (Array.isArray(data.priceHistory)) {
        for (const entry of data.priceHistory) {
          if (entry.sellPriceFormatted !== undefined) {
            delete entry.sellPriceFormatted;
            modified = true;
          }
          if (entry.buyPriceFormatted !== undefined) {
            delete entry.buyPriceFormatted;
            modified = true;
          }
          totalEntries++;
        }
      }

      // Clean latest object
      if (data.latest && typeof data.latest === 'object') {
        for (const city in data.latest) {
          for (const quality in data.latest[city]) {
            if (data.latest[city][quality].sellPriceFormatted !== undefined) {
              delete data.latest[city][quality].sellPriceFormatted;
              modified = true;
            }
            if (data.latest[city][quality].buyPriceFormatted !== undefined) {
              delete data.latest[city][quality].buyPriceFormatted;
              modified = true;
            }
          }
        }
      }

      // Write back if modified
      if (modified) {
        fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
        cleanedCount++;
      }
    } catch (e) {
      console.warn(`⚠️  Failed to process ${file}:`, e.message);
    }
  }

  console.log(`✅ Cleaned ${cleanedCount} files (${totalEntries} total entries processed)`);

  // Commit to git
  try {
    console.log('📤 Committing cleanup...');
    execSync('git config user.name "GitHub Actions"', { cwd: process.cwd() });
    execSync('git config user.email "actions@github.com"', { cwd: process.cwd() });
    execSync('git add data/prices/', { cwd: process.cwd() });
    
    try {
      const commitMsg = `Cleanup: Remove formatted price fields (keep raw numbers only)\n\n- Removed all sellPriceFormatted and buyPriceFormatted fields\n- Kept only raw numbers for calculations\n- Cleaned ${cleanedCount} files, ${totalEntries} entries`;
      execSync(`git commit -m "${commitMsg}"`, { cwd: process.cwd() });
      execSync('git push', { cwd: process.cwd() });
      console.log('✅ Cleanup committed and pushed!');
    } catch (commitErr) {
      console.log('ℹ️  No changes to commit');
    }
  } catch (e) {
    console.error('❌ Git error:', e.message);
  }

  console.log('✅ Cleanup complete!');
  process.exit(0);
}

cleanupPrices().catch((e) => {
  console.error('Fatal error:', e);
  process.exit(1);
});
