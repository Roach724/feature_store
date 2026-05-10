"""Build feature_store as wheel and zip for cluster deployment.

Usage:
    python scripts/build.py

Output:
    dist/feature_store-2.0.0-py3-none-any.whl
    dist/feature_store.zip
"""

import os
import shutil
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
DIST = os.path.join(ROOT, "dist")


def clean_dist():
    if os.path.exists(DIST):
        shutil.rmtree(DIST)
    os.makedirs(DIST)


def build_wheel():
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "build"],
        check=True, capture_output=True,
    )
    subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", DIST, ROOT],
        check=True,
    )


def build_zip():
    zip_path = os.path.join(DIST, "feature_store.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SRC):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for f in files:
                if f.endswith(".pyc"):
                    continue
                full = os.path.join(root, f)
                arcname = os.path.relpath(full, SRC)
                zf.write(full, arcname)
    return zip_path


def main():
    clean_dist()
    build_wheel()
    zip_path = build_zip()
    for f in sorted(os.listdir(DIST)):
        size = os.path.getsize(os.path.join(DIST, f))
        print(f"  dist/{f} ({size:,} bytes)")


if __name__ == "__main__":
    main()
