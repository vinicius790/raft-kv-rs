#!/usr/bin/env python3
"""Portable project commands. Run: python tools/dev.py test|build|package."""
from pathlib import Path
import argparse, os, subprocess, sys, zipfile
ROOT = Path(__file__).resolve().parents[1]
EXCLUDE = {".git", ".godot", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", "target", "node_modules", "dist", "build", "obj"}
def execute(args, env=None):
    subprocess.run(args, cwd=ROOT, env=env, check=True)
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["test","build","package"])
    args=parser.parse_args()
    if args.command=="package":
        destination=ROOT/"dist"/(ROOT.name+".zip");destination.parent.mkdir(exist_ok=True)
        with zipfile.ZipFile(destination,"w",zipfile.ZIP_DEFLATED) as out:
            for path in sorted(ROOT.rglob("*")):
                parts=path.relative_to(ROOT).parts
                if not path.is_file() or path.is_symlink() or any(p in EXCLUDE or p.endswith(".egg-info") for p in parts) or "HostTests/bin" in path.as_posix():continue
                if path.suffix in {".pyc",".pyo"}:continue
                out.write(path,Path(ROOT.name)/path.relative_to(ROOT))
        print(destination);return
    test=args.command=="test"
    if (ROOT/"Cargo.toml").exists():execute(["cargo","test" if test else "build","--locked"])
    elif (ROOT/"go.mod").exists():execute(["go","test" if test else "build","./..."])
    elif (ROOT/"CMakeLists.txt").exists():
        execute(["cmake","-S",".","-B","build"]);execute(["cmake","--build","build","--parallel"])
        if test:execute(["ctest","--test-dir","build","--output-on-failure"])
    elif (ROOT/"project.godot").exists():
        execute(["godot","--headless","--path",".","--editor","--quit"])
        if test:execute(["godot","--headless","--path",".","--script","tests/test_registry.gd"])
    elif (ROOT/"pyproject.toml").exists():
        if test:
            env=os.environ.copy();env["PYTHONPATH"]=os.pathsep.join([str(ROOT/"src"),str(ROOT),env.get("PYTHONPATH","")])
            execute([sys.executable,"-m","pytest","--import-mode=importlib"],env)
        else:execute([sys.executable,"-m","pip","wheel","--no-deps","--wheel-dir","dist",str(ROOT)])
    elif (ROOT/"HostTests/Anchor.HostTests.csproj").exists():execute(["dotnet","run","--project","HostTests/Anchor.HostTests.csproj"])
    else:
        parser.error("This plugin requires its engine SDK; use the editor build workflow in README.")
if __name__=="__main__":main()
