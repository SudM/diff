#!/usr/bin/env python3
"""
Generates a side-by-side HTML diff (Diffchecker style)
between two JSON files (line-by-line).
"""

import json, difflib, argparse
from pathlib import Path

def pretty_json(path: Path) -> str:
    """Return pretty-printed JSON text from a file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    return json.dumps(data, indent=2, ensure_ascii=False, sort_keys=False)

def main(prod: Path, final: Path, output: Path):
    old = pretty_json(prod).splitlines()
    new = pretty_json(final).splitlines()

    differ = difflib.HtmlDiff(wrapcolumn=120)
    html = differ.make_file(old, new,
                            fromdesc=str(prod.name),
                            todesc=str(final.name),
                            context=False, numlines=2)

    output.write_text(html, encoding="utf-8")
    print(f"✅ Diff report generated → {output.resolve()}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Produce Diffchecker-style JSON diff")
    p.add_argument("-p", "--prod", type=Path, default=Path("./output/package_toggle.json"))
    p.add_argument("-f", "--final", type=Path, default=Path("./output/final_package_toggle.json"))
    
    p.add_argument("-o", "--output", type=Path, default=Path("./output/DiffReport.html"))
    a = p.parse_args()
    main(a.prod, a.final, a.output)


