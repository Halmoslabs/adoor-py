"""Runs in the public adoor-py repository before PyPI publish. There is no PyPI token here."""

import re
import sys
import tomllib

with open("pyproject.toml", "rb") as handle:
    project = tomllib.load(handle)["project"]
tag = sys.argv[1] if len(sys.argv) > 1 else ""
problems = []
if project.get("name") != "adoor":
    problems.append("The package name must be adoor.")
if tag != "v" + project.get("version", ""):
    problems.append(f"Tag {tag} does not match pyproject version {project.get('version')}.")
with open("src/adoor/client.py", encoding="utf-8") as handle:
    if f'VERSION = "{project.get("version")}"' not in handle.read():
        problems.append("src/adoor/client.py VERSION does not match pyproject.toml.")
blob = open("pyproject.toml", encoding="utf-8").read()
if re.search(r"Halmoslabs[/]adoor(?![-\w])", blob) or re.search(r"@halmoslabs\.com", blob, re.I):
    problems.append("pyproject.toml points at the private repository.")
if project.get("dependencies"):
    problems.append("The package must have no required dependencies.")
if problems:
    print("\n".join(problems), file=sys.stderr)
    sys.exit(1)
print(f"adoor {project['version']} can be published.")
