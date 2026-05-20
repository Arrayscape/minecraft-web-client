#!/bin/bash
#
# Deploy script for the minecraft-web-client package. Bundled into the
# .zip produced by `make package`; meant to be run on the target VM as
# root after unzipping.
#
# Usage:
#   unzip mwc-<tag>.zip      # creates mwc-<tag>/
#   cd mwc-<tag>
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
NGINX_AVAILABLE=/etc/nginx/sites-available/mc.conf
NGINX_ENABLED=/etc/nginx/sites-enabled/mc.conf
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

# Install the new config to sites-available/, then make sure the
# sites-enabled/ symlink points at it. Debian/Ubuntu nginx convention:
# the real file lives in sites-available, sites-enabled is just symlinks
# for the currently-active subset.
#
# Order matters: install to sites-available BEFORE creating/refreshing
# the symlink, so `nginx -t` always sees a consistent state. If
# validation fails, restore the previous available file from backup.
nginx_backup=""
if [[ -f "$NGINX_AVAILABLE" ]]; then
    nginx_backup="$NGINX_AVAILABLE.bak.$(date +%s)"
    cp "$NGINX_AVAILABLE" "$nginx_backup"
fi

mkdir -p "$(dirname "$NGINX_AVAILABLE")" "$(dirname "$NGINX_ENABLED")"

echo "Installing nginx config to $NGINX_AVAILABLE ..."
cp nginx-mc.conf "$NGINX_AVAILABLE"

# Refuse to clobber an existing non-symlink at sites-enabled — that's
# probably a hand-edited deployment that someone will be unhappy to lose.
if [[ -e "$NGINX_ENABLED" && ! -L "$NGINX_ENABLED" ]]; then
    echo "Error: $NGINX_ENABLED exists and is not a symlink. Move it aside before deploying:" >&2
    echo "       sudo mv $NGINX_ENABLED $NGINX_ENABLED.preexisting" >&2
    if [[ -n "$nginx_backup" && -f "$nginx_backup" ]]; then
        cp "$nginx_backup" "$NGINX_AVAILABLE"
    fi
    exit 1
fi

echo "Linking $NGINX_ENABLED -> $NGINX_AVAILABLE ..."
ln -snf "$NGINX_AVAILABLE" "$NGINX_ENABLED"

echo "Validating nginx config ..."
if ! nginx -t; then
    echo "Error: nginx -t failed for the new config; rolling back" >&2
    if [[ -n "$nginx_backup" && -f "$nginx_backup" ]]; then
        cp "$nginx_backup" "$NGINX_AVAILABLE"
    fi
    # The symlink either already pointed at the available file or now does;
    # since we restored the available file's contents, the symlink target
    # is back to the previous state automatically.
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
