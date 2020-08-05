#/bin/bash

## Update code from Github

echo ""
echo "Running import site update at $(date)"
cd /data/tools/import

# Force reset and pull latest version.
/usr/bin/git reset --hard origin/master
/usr/bin/git pull

echo "Code updated completed at $(date)"
echo ""
