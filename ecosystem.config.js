const path = require('path');

module.exports = {
  apps: [{
    name: 'polymarket-bot-5m-market-binace-free-limit-fixed-shares',
    script: path.join(__dirname, 'target/release/polymarket-bot-5m-market-binace-free-limit-fixed-shares'),
    cwd: __dirname,
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '1G',
    env: {
      RUST_LOG: 'info'
    },
    error_file: path.join(__dirname, 'logs/pm2-error.log'),
    out_file: path.join(__dirname, 'logs/pm2-out.log')
  }]
};
