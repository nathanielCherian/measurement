#!/usr/bin/env bash
# Deploy the probe page (existing nginx + Let's Encrypt via certbot) and the
# WebTransport server (systemd) on Ubuntu.
#
#   sudo deploy/install.sh <domain> <acme-email> [udp-port]
#
# Prerequisites: <domain> has a DNS record pointing at this machine, nginx is
# running and reachable on TCP 80/443, and server/.venv exists.
# Only adds a new nginx site for <domain>; if `nginx -t` fails, that site is
# reverted so existing sites keep working.
# Safe to re-run: publishes updated web/ files and restarts the server.
set -euo pipefail

DOMAIN=${1:?usage: sudo deploy/install.sh <domain> <acme-email> [udp-port]}
EMAIL=${2:?usage: sudo deploy/install.sh <domain> <acme-email> [udp-port]}
PORT=${3:-4433}

die() { echo "error: $*" >&2; exit 1; }
[[ $EUID -eq 0 ]] || die "run with sudo"
RUN_USER=${SUDO_USER:-}
[[ -n $RUN_USER && $RUN_USER != root ]] || die "run via sudo from the account that owns the repo"
RUN_GROUP=$(id -gn "$RUN_USER")

REPO=$(cd "$(dirname "$0")/.." && pwd)
DEPLOY=$REPO/deploy
WEB_ROOT=/srv/browser-cc-probe/web
ACME_ROOT=/var/www/letsencrypt
LIVE=/etc/letsencrypt/live/$DOMAIN

command -v nginx >/dev/null || die "nginx not found"
[[ -x $REPO/server/.venv/bin/python ]] || die "missing $REPO/server/.venv (create it first)"
sudo -u "$RUN_USER" "$REPO/server/.venv/bin/python" -c "import aioquic" \
  || die "aioquic not installed in server/.venv"

if [[ -d /etc/nginx/sites-available && -d /etc/nginx/sites-enabled ]]; then
  SITE=/etc/nginx/sites-available/browser-cc-probe-$DOMAIN.conf
  SITE_LINK=/etc/nginx/sites-enabled/browser-cc-probe-$DOMAIN.conf
else
  SITE=/etc/nginx/conf.d/browser-cc-probe-$DOMAIN.conf
  SITE_LINK=
fi

render() {
  sed -e "s|@DOMAIN@|$DOMAIN|g" -e "s|@PORT@|$PORT|g" -e "s|@WEB_ROOT@|$WEB_ROOT|g" \
      -e "s|@ACME_ROOT@|$ACME_ROOT|g" -e "s|@REPO@|$REPO|g" -e "s|@RUN_USER@|$RUN_USER|g" \
      -e "s|@RUN_GROUP@|$RUN_GROUP|g" "$1" > "$2"
}

# Install an nginx site from a template, test the whole config, reload.
# On a failed test, put back the previous version of our site (or remove it)
# so the on-disk nginx config stays valid.
install_site() {
  local backup=
  if [[ -f $SITE ]]; then backup=$(mktemp); cp "$SITE" "$backup"; fi
  render "$1" "$SITE"
  [[ -z $SITE_LINK ]] || ln -sf "$SITE" "$SITE_LINK"
  if ! nginx -t; then
    if [[ -n $backup ]]; then
      mv "$backup" "$SITE"
    else
      rm -f "$SITE" ${SITE_LINK:+"$SITE_LINK"}
    fi
    die "nginx -t failed with the new site; reverted it (other sites untouched)"
  fi
  [[ -z $backup ]] || rm -f "$backup"
  systemctl reload nginx
}

echo "==> packages"
command -v certbot >/dev/null || { apt-get update && apt-get install -y certbot; }

echo "==> publishing web/ to $WEB_ROOT"
# Copied out of the home directory because Ubuntu home dirs are not readable by nginx.
rm -rf "$WEB_ROOT"
install -d -m 755 "$WEB_ROOT" "$ACME_ROOT"
cp -r "$REPO/web/." "$WEB_ROOT/"
rm -f "$WEB_ROOT/cert-hash.json"
chmod -R a+rX "$WEB_ROOT"

echo "==> firewall"
if command -v ufw >/dev/null && ufw status | grep -q "Status: active"; then
  ufw allow 80/tcp
  ufw allow 443/tcp
  ufw allow "$PORT/udp"
else
  echo "    ufw inactive; make sure TCP 80,443 and UDP $PORT are open (incl. any external firewall)"
fi

if [[ ! -f $LIVE/fullchain.pem ]]; then
  echo "==> obtaining Let's Encrypt certificate for $DOMAIN"
  install_site "$DEPLOY/nginx-acme.conf"
  certbot certonly --non-interactive --agree-tos -m "$EMAIL" \
    --webroot -w "$ACME_ROOT" -d "$DOMAIN"
fi

echo "==> nginx site $SITE"
install_site "$DEPLOY/nginx-site.conf"

echo "==> certificate for the probe server"
install -d -m 750 -o root -g "$RUN_GROUP" /etc/browser-cc-probe
HOOK=/etc/letsencrypt/renewal-hooks/deploy/browser-cc-probe-$DOMAIN.sh
install -d /etc/letsencrypt/renewal-hooks/deploy
render "$DEPLOY/certbot-deploy-hook.sh" "$HOOK"
chmod 755 "$HOOK"

echo "==> systemd"
render "$DEPLOY/browser-cc-probe.service" /etc/systemd/system/browser-cc-probe.service
systemctl daemon-reload
systemctl enable browser-cc-probe.service
# The hook copies the cert and restarts the service if it is already running.
RENEWED_LINEAGE=$LIVE "$HOOK"
systemctl is-active --quiet browser-cc-probe.service || systemctl start browser-cc-probe.service

sleep 1
systemctl --no-pager --lines=5 status browser-cc-probe.service || true
echo
echo "done: open https://$DOMAIN/  (WebTransport server: https://$DOMAIN:$PORT/probe)"
