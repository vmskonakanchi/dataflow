#!/usr/bin/env python3
"""Automates version bumping across pyproject.toml, Dockerfile, CHANGELOG.md, and uv.lock."""

import sys
import os
import re
import subprocess
from datetime import date

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/bump_version.py <new_version>")
        print("Example: python scripts/bump_version.py 0.3.0")
        sys.exit(1)

    new_version = sys.argv[1].strip().lstrip("v")
    # Simple semver regex validation
    if not re.match(r"^\d+\.\d+\.\d+$", new_version):
        print(f"Error: '{new_version}' is not a valid semver version (e.g. 0.3.0)")
        sys.exit(1)

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    pyproject_path = os.path.join(root_dir, "pyproject.toml")
    dockerfile_path = os.path.join(root_dir, "Dockerfile")
    changelog_path = os.path.join(root_dir, "CHANGELOG.md")

    # 1. Read current version from pyproject.toml
    if not os.path.exists(pyproject_path):
        print(f"Error: Could not find pyproject.toml at {pyproject_path}")
        sys.exit(1)

    with open(pyproject_path, "r") as f:
        pyproject_content = f.read()

    match = re.search(r'^version\s*=\s*"([^"]+)"', pyproject_content, re.MULTILINE)
    if not match:
        print("Error: Could not find current version in pyproject.toml")
        sys.exit(1)

    old_version = match.group(1)
    if old_version == new_version:
        print(f"Version is already {new_version}. Nothing to do.")
        sys.exit(0)

    print(f"Bumping version from {old_version} to {new_version}...")

    # 2. Update pyproject.toml
    pyproject_content = re.sub(
        r'^version\s*=\s*"[^"]+"',
        f'version = "{new_version}"',
        pyproject_content,
        flags=re.MULTILINE
    )
    with open(pyproject_path, "w") as f:
        f.write(pyproject_content)
    print("✓ Updated pyproject.toml")

    # 3. Update Dockerfile
    if os.path.exists(dockerfile_path):
        with open(dockerfile_path, "r") as f:
            docker_content = f.read()
        docker_content = re.sub(
            r'^ARG VERSION=[^\n]+',
            f'ARG VERSION={new_version}',
            docker_content,
            flags=re.MULTILINE
        )
        with open(dockerfile_path, "w") as f:
            f.write(docker_content)
        print("✓ Updated Dockerfile")

    # 4. Update CHANGELOG.md
    if os.path.exists(changelog_path):
        with open(changelog_path, "r") as f:
            changelog_content = f.read()

        # Update Unreleased header to release header
        today_str = date.today().isoformat()
        changelog_content = re.sub(
            r'^## \[Unreleased\]',
            f'## [Unreleased]\n\n## [{new_version}] — {today_str}',
            changelog_content,
            flags=re.MULTILINE
        )

        # Update markdown links at the bottom
        # Replace Unreleased link
        changelog_content = re.sub(
            r'^\[Unreleased\]:\s*https://github\.com/[^/]+/[^/]+/compare/v[^\.]+\.[^\.]+\.[^\.]+[^/]*\.\.\.main',
            f'[Unreleased]: https://github.com/vmskonakanchi/dataflow/compare/v{new_version}...main',
            changelog_content,
            flags=re.MULTILINE
        )
        # Add new version link right below Unreleased
        unreleased_link_re = r'^(\[Unreleased\]:[^\n]+)'
        new_link = f'[{new_version}]: https://github.com/vmskonakanchi/dataflow/releases/tag/v{new_version}'
        changelog_content = re.sub(
            unreleased_link_re,
            f'\\1\n{new_link}',
            changelog_content,
            flags=re.MULTILINE
        )

        with open(changelog_path, "w") as f:
            f.write(changelog_content)
        print("✓ Updated CHANGELOG.md")

    # 5. Sync uv lock file to lock the new version
    print("Running 'uv lock' to update lock file...")
    try:
        subprocess.run(["uv", "lock"], check=True, cwd=root_dir)
        print("✓ Updated uv.lock")
    except Exception as e:
        print(f"Warning: Failed to run 'uv lock': {e}")

    print("\nSuccessfully bumped version!")
    print("Next steps:")
    print("  git add pyproject.toml Dockerfile CHANGELOG.md uv.lock")
    print(f"  git commit -m \"release: v{new_version}\"")
    print(f"  git tag v{new_version}")

if __name__ == "__main__":
    main()
