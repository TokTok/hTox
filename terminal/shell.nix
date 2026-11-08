# hTox in a terminal: `nix-shell --run 'cabal run htox'`. Cabal builds
# Hatter's terminal platform from its checkout too.
let
  hatter = import ../nix/hatter.nix;
  inherit (hatter) pkgs;
  inherit (pkgs.haskell.lib) dontCheck dontHaddock disableLibraryProfiling;
  # Like the libraries packages.nix builds.
  plainly = p: disableLibraryProfiling (dontHaddock (dontCheck p));
  hpkgs = (import "${hatter.src}/nix/hpkgs.nix" { inherit pkgs; }).extend
    (pkgs.lib.composeExtensions (import ../nix/packages.nix pkgs) (self: super: {
      hatter = dontHaddock (dontCheck super.hatter-project);
      direct-sqlite = import ../nix/sqlcipher.nix { inherit pkgs; inherit (pkgs) openssl; link = true; } super.direct-sqlite;
      hatter-terminal = plainly (self.callCabal2nix "hatter-terminal" (pkgs.lib.cleanSourceWith { src = hatter.terminal; filter = path: _: baseNameOf path != "dist-newstyle"; }) { });
      htox = plainly (self.callCabal2nix "htox" (hatter.app pkgs) { });
      htox-terminal = self.callCabal2nix "htox-terminal" ./. { };
    }));
in
hpkgs.shellFor {
  packages = ps: [ ps.htox-terminal ps.hatter-terminal ];
  nativeBuildInputs = [ pkgs.cabal-install ];
}
