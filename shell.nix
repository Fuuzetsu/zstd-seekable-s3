{ pkgs ? import ./nix/nixpkgs.nix }:

with pkgs;

mkShell {
  buildInputs = [ rustup pkgconfig openssl.dev zstd xxHash ];
  LANG = "C.UTF-8";
}
