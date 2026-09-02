const fs = require('fs');
const path = require('path');

const source = path.resolve(__dirname, '..', 'src', 'config', 'log_config.json');
const destination = path.resolve(__dirname, '..', 'dist', 'config', 'log_config.json');

fs.mkdirSync(path.dirname(destination), { recursive: true });
fs.copyFileSync(source, destination);
