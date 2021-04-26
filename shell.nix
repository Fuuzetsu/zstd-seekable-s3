# This is just a compat shim for nix flakes: to add things to the shell, look in
# `flake.nix` instead.
(import (let lock = builtins.fromJSON (builtins.readFile ./flake.lock);
in fetchTarball {
  url =
    "https://github.com/edolstra/flake-compat/archive/${lock.nodes.flake-compat.locked.rev}.tar.gz";
  sha256 = lock.nodes.flake-compat.locked.narHash;
}) { src = ./.; }).shellNix
