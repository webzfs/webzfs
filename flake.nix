{
  description = "WebZFS - Web-based ZFS management interface and NixOS module";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      pkgsFor = system: nixpkgs.legacyPackages.${system};
    in
    {
      packages = forAllSystems (system: rec {
        webzfs = (pkgsFor system).callPackage ./ports/nix/package.nix { };
        default = webzfs;
      });

      devShells = forAllSystems (system: {
        default = import ./ports/nix/dev-shell.nix { pkgs = pkgsFor system; };
      });

      nixosModules = rec {
        webzfs = import ./ports/nix/module.nix;
        default = webzfs;
      };
    };
}
