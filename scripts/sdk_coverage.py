"""Summarize source line and branch coverage for the Go SDK module."""

import argparse
from collections import defaultdict
import re
from pathlib import Path
import subprocess


BLOCK = re.compile(r"(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)")
BRANCH_SUMMARY = re.compile(r"^Branch coverage: (\d+)/(\d+)$", re.MULTILINE)


def line_counts(profile, module, directory):
    """Count each source line once, even when it contains overlapping Go blocks."""
    counts = defaultdict(dict)
    sources = {}
    for entry in profile.read_text().splitlines()[1:]:
        filename, start_line, start_col, end_line, end_col, statements, hits = BLOCK.fullmatch(entry).groups()
        path = filename.removeprefix(module + "/")
        if path == filename or int(statements) == 0:
            continue
        if path not in sources:
            sources[path] = (directory / path).read_bytes().splitlines()
        lines = sources[path]
        start_line, start_col, end_line, end_col = map(int, (start_line, start_col, end_line, end_col))
        package = str(Path(path).parent)
        for number in range(start_line, end_line + 1):
            start = start_col - 1 if number == start_line else 0
            end = end_col - 1 if number == end_line else None
            code = lines[number - 1][start:end].strip(b" \t{}(),;")
            if not code or code.startswith((b"//", b"/*", b"* ")):
                continue
            key = path, number
            counts[package][key] = counts[package].get(key, False) or int(hits) > 0
    return {package: (sum(lines.values()), len(lines)) for package, lines in counts.items()}


def branch_counts(output, packages):
    """Read the independent Gobco report for each package."""
    counts = {}
    for package in packages:
        name = package.replace("/", "-")
        log = output / f"{name}.branches.log"
        text = log.read_text()
        match = BRANCH_SUMMARY.search(text)
        if match is None or re.search(r"^FAIL\b", text, re.MULTILINE):
            raise SystemExit(f"Branch coverage failed for {package}. See {log}")
        counts[package] = tuple(map(int, match.groups()))
    return counts


def report(covered, total):
    return f"{covered}/{total} ({100 * covered / total:.2f}%)" if total else "0/0"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path)
    parser.add_argument("--release", action="store_true")
    args = parser.parse_args()
    covered_lines = total_lines = covered_branches = total_branches = 0
    print(f"{'Package':<24} {'Source lines':>21} {'Gobco branches':>21}")
    for directory in (Path("."), Path("tea")):
        output = args.reports / directory
        module = subprocess.check_output(["go", "list", "-m"], text=True, cwd=directory).strip()
        paths = subprocess.check_output(["go", "list", "./..."], text=True, cwd=directory).splitlines()
        packages = [path.removeprefix(module + "/") for path in paths]
        lines = line_counts(output / "go.cover", module, directory)
        branches = branch_counts(output, packages)
        for package in packages:
            line_hits, line_total = lines.get(package, (0, 0))
            branch_hits, branch_total = branches[package]
            covered_lines += line_hits
            total_lines += line_total
            covered_branches += branch_hits
            total_branches += branch_total
            print(f"{str(directory / package):<24} "
                  f"{report(line_hits, line_total):>21} {report(branch_hits, branch_total):>21}")
    print(f"{'SDK total':<24} {report(covered_lines, total_lines):>21} {report(covered_branches, total_branches):>21}")
    if args.release and (
        100 * covered_branches <= 90 * total_branches or 100 * covered_lines <= 85 * total_lines
    ):
        raise SystemExit("Release requires more than 90% branch coverage and more than 85% line coverage across the SDK.")


if __name__ == "__main__":
    main()
