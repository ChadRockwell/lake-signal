#!/usr/bin/env python3
"""Deploy Databricks Asset Bundles in the example directory.

Deploys all bundles by default. Optionally destroy before deploying,
or destroy only. Bundles are processed in dependency order.
"""

import argparse
import subprocess
import sys
from pathlib import Path

EXAMPLE_DIR = Path(__file__).resolve().parent

# Bundles in deployment order (destroy reverses this).
# Each entry: (logical_name, relative_path_from_example_dir)
BUNDLES = [
    ("bronze_loader", "bronze_loader"),
    ("inline/silver", "inline/silver"),
    ("inline/gold", "inline/gold"),
    ("dispatch/silver", "dispatch/silver"),
    ("dispatch/gold", "dispatch/gold"),
    ("dispatch/dispatcher", "dispatch/dispatcher"),
]


def run_bundle_command(
    action: str,
    bundle_path: Path,
    target: str,
    dry_run: bool,
) -> bool:
    """Run a databricks bundle command and return True on success."""
    cmd = ["databricks", "bundle", action, "--target", target]
    if action == "destroy":
        cmd.append("--auto-approve")

    label = f"{action:>7s} | {bundle_path.relative_to(EXAMPLE_DIR)} (target={target})"

    if dry_run:
        print(f"  [dry-run] {label}")
        print(f"            cmd: {' '.join(cmd)}")
        return True

    print(f"  {label}")
    result = subprocess.run(cmd, cwd=bundle_path)
    if result.returncode != 0:
        print(f"  ✗ FAILED  {label}", file=sys.stderr)
        return False
    print(f"  ✓ OK      {label}")
    return True


def resolve_bundles(names: list[str] | None) -> list[tuple[str, Path]]:
    """Return the selected bundles as (name, absolute_path) pairs."""
    if names is None:
        return [(n, EXAMPLE_DIR / p) for n, p in BUNDLES]

    selected = []
    for requested in names:
        matched = [
            (n, EXAMPLE_DIR / p)
            for n, p in BUNDLES
            if requested == n or n.startswith(requested + "/") or requested == p
        ]
        if not matched:
            print(f"Unknown bundle: {requested}", file=sys.stderr)
            print(f"Available: {', '.join(n for n, _ in BUNDLES)}", file=sys.stderr)
            sys.exit(1)
        selected.extend(matched)

    # Deduplicate while preserving order.
    seen = set()
    deduped = []
    for item in selected:
        if item[0] not in seen:
            seen.add(item[0])
            deduped.append(item)
    return deduped


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deploy Databricks Asset Bundles in the example directory.",
    )
    parser.add_argument(
        "--destroy-first",
        action="store_true",
        help="Destroy all bundles before deploying.",
    )
    parser.add_argument(
        "--destroy-only",
        action="store_true",
        help="Only destroy bundles, do not deploy.",
    )
    parser.add_argument(
        "-t",
        "--target",
        default="dev",
        help="Databricks bundle target (default: dev).",
    )
    parser.add_argument(
        "-b",
        "--bundles",
        nargs="+",
        metavar="NAME",
        help=(
            "Specific bundles or groups to process. "
            "Use logical names (e.g. 'bronze_loader', 'inline/silver') "
            "or group prefixes ('inline', 'dispatch'). "
            "Default: all bundles."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without executing.",
    )
    args = parser.parse_args()

    if args.destroy_first and args.destroy_only:
        parser.error("--destroy-first and --destroy-only are mutually exclusive.")

    bundles = resolve_bundles(args.bundles)
    if not bundles:
        print("No bundles matched.", file=sys.stderr)
        sys.exit(1)

    do_destroy = args.destroy_first or args.destroy_only
    do_deploy = not args.destroy_only

    print(f"Target : {args.target}")
    print(f"Action : {'destroy → deploy' if (do_destroy and do_deploy) else 'deploy' if do_deploy else 'destroy'}")
    print(f"Bundles: {', '.join(n for n, _ in bundles)}")
    print()

    # --- Destroy (reverse order) ---
    if do_destroy:
        print("=== Destroying bundles ===")
        for name, path in reversed(bundles):
            if not run_bundle_command("destroy", path, args.target, args.dry_run):
                print("Aborting due to destroy failure.", file=sys.stderr)
                sys.exit(1)
        print()

    # --- Deploy (forward order) ---
    if do_deploy:
        print("=== Deploying bundles ===")
        for name, path in bundles:
            if not run_bundle_command("deploy", path, args.target, args.dry_run):
                print("Aborting due to deploy failure.", file=sys.stderr)
                sys.exit(1)
        print()

    print("Done.")


if __name__ == "__main__":
    main()
