import { initBuMap, buNameIdMap } from './src/common.js';
(async () => {
  await initBuMap();
  const names = Object.keys(buNameIdMap || {}).sort();
  console.log(names.length ? names.join('\n') : '(no BUs discovered)');
})().catch(e => { console.error(e); process.exit(1); });
