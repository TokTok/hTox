# Desktop build and tests (Hatter's platform calls are stubs there):
# `nix-shell --run 'cabal test'`.
let
  hatter = import ./nix/hatter.nix;
  inherit (hatter) pkgs;
  hpkgs = (import "${hatter.src}/nix/hpkgs.nix" { inherit pkgs; }).extend
    (pkgs.lib.composeExtensions (import ./nix/packages.nix pkgs) (self: super: {
      hatter = pkgs.haskell.lib.dontHaddock
        (pkgs.haskell.lib.dontCheck super.hatter-project);
      htox = self.callCabal2nix "htox" (hatter.app pkgs) { };
    }));
in
hpkgs.shellFor {
  packages = ps: [ ps.htox ];
  nativeBuildInputs = [ pkgs.cabal-install ];
}
