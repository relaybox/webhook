const esbuild = require('esbuild');
const path = require('path');
const fs = require('fs');
const pkg = require('./package.json');

esbuild
  .build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    platform: 'node',
    target: 'node20',
    outfile: 'build/index.js',
    // outdir: 'build',
    external: [...Object.keys(pkg.dependencies || {})],
    plugins: [],
    minify: true,
    sourcemap: true
  })
  .then(() => {
    const srcCertsDir = path.resolve(__dirname, 'src', 'certs');
    const destCertsDir = path.resolve(__dirname, 'build', 'certs');

    try {
      fs.cpSync(srcCertsDir, destCertsDir, { recursive: true });
      console.log('Certs directory copied successfully.');
    } catch (err) {
      console.error('Error copying certs directory:', err);
      process.exit(1);
    }
  })
  .catch(() => process.exit(1));
