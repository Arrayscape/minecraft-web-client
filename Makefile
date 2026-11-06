#
TAG := $(shell git describe --tags --exact-match 2>/dev/null)

all:
	pnpm install
	pnpm build

clean:
	rm -rf dist node_modules

# package builds from scratch and checks before zipping. dist/ is shared
# with the dev tooling — `pnpm start`'s rsbuild dev server and mesher
# watcher write into it — so a package built beside a running dev server
# shipped its HMR files (v0.1.99.auto.1, first attempt). The build now
# starts from an empty dist/, refuses to run while this checkout's dev
# tooling is up, and fails if dev artifacts appear anyway.
package:
	@if pgrep -f "$(CURDIR)/node_modules" >/dev/null; then \
		echo "Error: this checkout's dev tooling is running (pgrep -fl '$(CURDIR)/node_modules'); it writes into dist/. Stop it before packaging." >&2; \
		exit 1; \
	fi
	@if [ -z "$(TAG)" ]; then \
		echo "Error: No git tag found on the latest commit." >&2; \
		exit 1; \
	fi
	@if ! git diff-index --quiet HEAD --; then \
		echo "Error: There are uncommitted changes in the repository." >&2; \
		exit 1; \
	fi
	@if [ ! -f secrets/config.json ]; then \
		echo "Error: secrets/config.json not found. Required for packaging." >&2; \
		exit 1; \
	fi
	@if [ ! -f secrets/nginx-mc.conf ]; then \
		echo "Error: secrets/nginx-mc.conf not found. Required for packaging." >&2; \
		exit 1; \
	fi
	rm -rf dist
	pnpm install
	pnpm build
	@if find dist -name '*.hot-update.*' | grep -q .; then \
		echo "Error: dev-server artifacts in dist/ after a clean build — something wrote into it during the build:" >&2; \
		find dist -name '*.hot-update.*' >&2; \
		exit 1; \
	fi
	rm -rf pkg.tmp
	# Stage everything under an outer mwc-<tag>/ directory so that
	# `unzip` on the target creates one clearly-named directory rather
	# than dumping files into the cwd. Final zip layout:
	#
	#   mwc-<tag>/
	#       deploy.sh
	#       nginx-mc.conf
	#       mwc-<tag>/        ← SPA tree (rsbuild dist) with secrets/config.json overlay
	#           index.html
	#           config.json
	#           static/...
	#
	mkdir -p pkg.tmp/mwc-$(TAG)
	cp -r dist pkg.tmp/mwc-$(TAG)/mwc-$(TAG)
	# Overlay deploy-specific config (magicLinkBackend, splash text, etc.)
	# on top of the public default that rsbuild copied from ./config.json.
	cp secrets/config.json pkg.tmp/mwc-$(TAG)/mwc-$(TAG)/config.json
	# Bundle the nginx config alongside the SPA tree so deploy.sh
	# can copy it to /etc/nginx/sites-enabled/mc.conf.
	cp secrets/nginx-mc.conf pkg.tmp/mwc-$(TAG)/nginx-mc.conf
	# Bundle the deploy script and preserve its executable bit through
	# the zip (so the operator doesn't have to chmod after unzipping).
	install -m 0755 deploy.sh pkg.tmp/mwc-$(TAG)/deploy.sh
	cd pkg.tmp && zip -r ../mwc-$(TAG).zip mwc-$(TAG)


