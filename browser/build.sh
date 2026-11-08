#!/bin/sh
# Builds hTox's page into a directory, from shell.nix's shell: hTox for
# wasm32-wasi in Hatter's browser host.
set -eu

out=$1
here=$(cd "$(dirname "$0")" && pwd)
cd "$here"

# monad-logger as shell.nix patches it, where cabal.project takes it.
mkdir -p dist-newstyle
rm -rf dist-newstyle/monad-logger-0.3.42
cp -rp "$MONAD_LOGGER" dist-newstyle/monad-logger-0.3.42
chmod -R u+w dist-newstyle/monad-logger-0.3.42
wasm32-wasi-cabal build exe:htox
"$here/../../third_party/hatter/browser/page.sh" "$(wasm32-wasi-cabal list-bin exe:htox)" "$out"
node "$here/icons.mjs" "$here/../platform/res/drawable" "$out/res"
