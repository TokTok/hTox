# direct-sqlite with SQLCipher instead of SQLite, so chats are encrypted at
# rest. The amalgamation is plain C, generated on the build machine. With
# `link`, the package links libcrypto itself (Android links it into the
# app's library instead).
{ pkgs, openssl, link ? false }:
let
  sqlcipher = pkgs.sqlcipher.overrideAttrs {
    pname = "sqlcipher-amalgamation";
    outputs = [ "out" ];
    buildFlags = [ "sqlite3.c" ];
    doCheck = false;
    installPhase = ''
      mkdir -p $out
      cp sqlite3.c sqlite3.h $out/
    '';
  };
  # Android's headers bring these in, glibc's don't.
  prelude = pkgs.lib.optionalString link "'#define _GNU_SOURCE 1' '#include <stdint.h>' ";
in
direct-sqlite: pkgs.haskell.lib.overrideCabal direct-sqlite (old: {
  postPatch = (old.postPatch or "") + ''
    cp ${sqlcipher}/sqlite3.h cbits/sqlite3.h
    { printf '%s\n' ${prelude}\
        '#define SQLITE_HAS_CODEC 1' \
        '#define SQLITE_EXTRA_INIT sqlcipher_extra_init' \
        '#define SQLITE_EXTRA_SHUTDOWN sqlcipher_extra_shutdown' \
        '#define SQLITE_THREADSAFE 1' \
        '#define SQLITE_TEMP_STORE 2' \
        '#define SQLCIPHER_CRYPTO_OPENSSL 1'
      cat ${sqlcipher}/sqlite3.c; } > cbits/sqlite3.c
  '' + pkgs.lib.optionalString link ''
    sed -i 's/^library$/library\n  extra-libraries: crypto/' direct-sqlite.cabal
  '';
  configureFlags = (old.configureFlags or [ ]) ++ [ "--extra-include-dirs=${openssl.dev}/include" ]
    ++ pkgs.lib.optional link "--extra-lib-dirs=${openssl.out}/lib";
})
