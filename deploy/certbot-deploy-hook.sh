#!/bin/sh
# Installed by deploy/install.sh into /etc/letsencrypt/renewal-hooks/deploy/.
# certbot runs every deploy hook after any renewal, so act only on @DOMAIN@:
# copy the new cert where the probe server (running as @RUN_USER@) can read it,
# then restart it.
set -e
[ "$RENEWED_LINEAGE" = "/etc/letsencrypt/live/@DOMAIN@" ] || exit 0
install -m 0644 -o root -g @RUN_GROUP@ "$RENEWED_LINEAGE/fullchain.pem" /etc/browser-cc-probe/cert.pem
install -m 0640 -o root -g @RUN_GROUP@ "$RENEWED_LINEAGE/privkey.pem" /etc/browser-cc-probe/key.pem
systemctl try-restart browser-cc-probe.service
