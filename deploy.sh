#!/bin/bash
#
# Deploy script for the minecraft-web-client package. Bundled into the
# .zip produced by `make package`; meant to be run on the target VM as
# root after unzipping.
#
# Usage:
#   scp mwc-<tag>.zip vm:
#   ssh vm
#   mkdir /tmp/mwc-deploy && cd /tmp/mwc-deploy
#   unzip ~/mwc-<tag>.zip
#   sudo ./deploy.sh
#
# What it does:
#   1. Copies the bundled mwc-<tag>/ tree to /var/www/mwc/mwc-<tag>/.
#   2. Validates the bundled nginx-mc.conf via `nginx -t`.
#   3. Atomically swaps /var/www/mwc/mwc → mwc-<tag>/ via a temp-name +
#      `mv -T`. Old versions stay on disk so rollback is just another
#      symlink swap (see "Rollback" below).
#   4. Reloads nginx.
#
# Rollback (no script needed):
#   sudo ln -snf mwc-<old-tag> /var/www/mwc/mwc.new
#   sudo mv -T /var/www/mwc/mwc.new /var/www/mwc/mwc
#   sudo systemctl reload nginx
#

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
    echo "Error: deploy.sh must be run as root (try: sudo $0)" >&2
    exit 1
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$script_dir"

# Find the mwc-<tag>/ directory in the bundle. There should be exactly one.
shopt -s nullglob
mwc_dirs=( mwc-* )
shopt -u nullglob
if [[ ${#mwc_dirs[@]} -ne 1 || ! -d "${mwc_dirs[0]}" ]]; then
    echo "Error: expected exactly one mwc-<tag>/ directory in the bundle, found: ${mwc_dirs[*]:-<none>}" >&2
    exit 1
fi
mwc_dir="${mwc_dirs[0]}"

if [[ ! -f nginx-mc.conf ]]; then
    echo "Error: nginx-mc.conf not found in bundle (looked in $script_dir)" >&2
    exit 1
fi

VAR_WWW=/var/www/mwc
NGINX_CONF=/etc/nginx/sites-enabled/mc.conf
SPA_LINK="$VAR_WWW/mwc"
SPA_LINK_TMP="$VAR_WWW/mwc.new"

mkdir -p "$VAR_WWW"

# Refuse to clobber if $SPA_LINK exists but isn't a symlink — it's probably
# a real directory holding the running SPA from a pre-symlink deploy.
# Operator must move it out of the way before we can take over.
if [[ -e "$SPA_LINK" && ! -L "$SPA_LINK" ]]; then
    echo "Error: $SPA_LINK exists and is not a symlink. Move it aside before deploying:" >&2
    echo "       sudo mv $SPA_LINK $SPA_LINK.preexisting" >&2
    exit 1
fi

# Capture the current symlink target for the post-deploy summary.
previous_target=""
if [[ -L "$SPA_LINK" ]]; then
    previous_target="$(readlink "$SPA_LINK")"
fi

target="$VAR_WWW/$mwc_dir"
if [[ -e "$target" ]]; then
    if [[ "$previous_target" == "$mwc_dir" ]]; then
        echo "Note: $mwc_dir is already the active version; refreshing files in place"
    else
        echo "Note: $target already exists; refreshing it"
    fi
    rm -rf "$target"
fi

echo "Installing SPA tree to $target ..."
cp -r "$mwc_dir" "$target"

# Validate the new nginx config against the current system before swapping
# anything live. nginx -t reads /etc/nginx/* in its current state, so we
# put the new config into place first but defer the symlink swap and
# reload until validation passes. If validation fails we restore the old
# nginx config from a backup.
nginx_backup=""
if [[ -f "$NGINX_CONF" ]]; then
    nginx_backup="$NGINX_CONF.bak.$(date +%s)"
    cp "$NGINX_CONF" "$nginx_backup"
fi

echo "Installing nginx config to $NGINX_CONF ..."
cp nginx-mc.conf "$NGINX_CONF"

echo "Validating nginx config ..."
if ! nginx -t; then
    echo "Error: nginx -t failed for the new config; rolling back nginx config" >&2
    if [[ -n "$nginx_backup" && -f "$nginx_backup" ]]; then
        cp "$nginx_backup" "$NGINX_CONF"
    fi
    echo "       (SPA symlink left unchanged; previous version still serving)" >&2
    exit 1
fi

# Atomically swap the symlink. `ln -snf` on its own isn't strictly atomic
# across all filesystems; using a temp name + `mv -T` is.
echo "Pointing $SPA_LINK -> $mwc_dir ..."
ln -snf "$mwc_dir" "$SPA_LINK_TMP"
mv -T "$SPA_LINK_TMP" "$SPA_LINK"

echo "Reloading nginx ..."
systemctl reload nginx

if [[ -n "$previous_target" ]]; then
    echo "Deployed: $mwc_dir   (previous: $previous_target — left on disk for rollback)"
else
    echo "Deployed: $mwc_dir   (first install)"
fi
