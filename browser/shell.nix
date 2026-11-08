# A shell for building hTox's page with build.sh: GHC's wasm toolchain
# (with Node.js for Hatter's host), libopus and libsodium for wasm32-wasi,
# and monad-logger's source as the build takes it.
{ pkgs ? import <nixpkgs> { } }:
let
  ghcWasm = (builtins.getFlake "gitlab:haskell-wasm/ghc-wasm-meta/8079eb96bbc63823d99ad8acaff0765532625474?host=gitlab.haskell.org").packages.${pkgs.stdenv.hostPlatform.system};

  # A static C library for wasm32-wasi. GHC links it into a shared one as
  # well, so it's position independent.
  wasi = drv: flags: pkgs.stdenvNoCC.mkDerivation {
    pname = "${drv.pname}-wasm32-wasi";
    inherit (drv) version src;
    nativeBuildInputs = [ ghcWasm.all_9_10 ];
    env.CFLAGS = "-O2 -fPIC";
    dontDisableStatic = true;
    configureFlags = [
      "--host=wasm32-wasi"
      "CC=wasm32-wasi-clang"
      "LD=wasm-ld"
      "AR=llvm-ar"
      "RANLIB=llvm-ranlib"
      "--disable-shared"
    ] ++ flags;
  };

  opus = wasi pkgs.libopus [
    "--disable-doc"
    "--disable-extra-programs"
    "--disable-stack-protector"
    "--disable-rtcd"
    "--disable-intrinsics"
  ];
  sodium = wasi pkgs.libsodium [
    "--disable-pie"
    "--disable-ssp"
    "--without-pthreads"
  ];
  # monad-logger as the build takes it (monad-logger.patch).
  monadLogger = pkgs.runCommand "monad-logger-0.3.42-for-wasm" { } ''
    tar xzf ${pkgs.haskellPackages.monad-logger.src}
    ${pkgs.gnupatch}/bin/patch -p0 < ${./monad-logger.patch}
    mv monad-logger-0.3.42 $out
  '';
in
pkgs.mkShell {
  packages = [ ghcWasm.all_9_10 pkgs.pkg-config ];
  PKG_CONFIG_LIBDIR = "${opus}/lib/pkgconfig:${sodium}/lib/pkgconfig";
  MONAD_LOGGER = monadLogger;
}
