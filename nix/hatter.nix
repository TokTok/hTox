# Hatter and the nixpkgs it's built and tested with. Our fixes are in the
# workspace clone until they're merged.
let
  src = builtins.path {
    name = "hatter";
    path = ../../third_party/hatter;
    filter = path: _: baseNameOf path != ".git" && baseNameOf path != "dist-newstyle"
      && path != toString terminal && path != toString browser;
  };
  # Its terminal platform, for running the app on a desktop.
  terminal = ../../third_party/hatter/terminal;
  # Its browser platform, with the host's npm packages.
  browser = ../../third_party/hatter/browser;
  sources = import "${src}/npins";
in
{
  inherit src sources terminal;
  pkgs = import sources.nixpkgs { };
  app = pkgs: pkgs.lib.fileset.toSource {
    root = ../.;
    fileset = pkgs.lib.fileset.unions [
      ../htox.cabal
      ../src
      ../test
    ];
  };
}
