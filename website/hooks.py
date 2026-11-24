"""MkDocs hooks that publish the numbered docs/ tree as the Workhold site.

The docs/ tree stays readable on GitHub as plain Markdown. This module adapts
it for MkDocs without changing any file under docs/:

- builds the navigation from the numbered section folders;
- serves website/landing.md at the site root and moves docs/README.md to /docs/;
- rewrites links that leave docs/ (or point into the excluded memory bank)
  to their GitHub location, so the strict build has no broken links.
"""

from __future__ import annotations

import posixpath
import re
from pathlib import Path
from typing import Any

from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import File, Files
from mkdocs.structure.pages import Page

WEBSITE_DIR = Path(__file__).resolve().parent
LANDING_SOURCE = WEBSITE_DIR / "landing.md"
REPO_BLOB = "https://github.com/KoninMikhail/workhold-durable-work-queue/blob/main/"
REPO_TREE = "https://github.com/KoninMikhail/workhold-durable-work-queue/tree/main/"

EXCLUDED_DIRS = ("_ai",)

SECTION_TITLES = {
    "00-onboarding": "Getting started",
    "01-concepts": "Concepts",
    "02-guides": "Guides",
    "03-reference": "Reference",
    "04-architecture": "Architecture",
    "05-operations": "Operations",
    "06-faq": "FAQ",
    "07-troubleshooting": "Troubleshooting",
    "08-examples": "Examples",
    "adr": "Decision records",
}

# Top-level tabs. Each tab lists docs/ section folders in display order.
TABS: list[tuple[str, list[str]]] = [
    (
        "Documentation",
        [
            "00-onboarding",
            "01-concepts",
            "02-guides",
            "08-examples",
            "06-faq",
            "07-troubleshooting",
        ],
    ),
    ("Architecture", ["04-architecture"]),
    ("Operations", ["05-operations"]),
    ("Reference", ["03-reference"]),
]

FENCE = re.compile(r"^(\s*)(`{3,}|~{3,})")
INLINE_LINK = re.compile(r"(\]\()(<[^>]+>|[^)\s]+)((?:\s+\"[^\"]*\")?\))")
REFERENCE_LINK = re.compile(r"^(\s{0,3}\[[^\]]+\]:\s*)(\S+)(.*)$")
EXTERNAL = re.compile(r"^(?:[a-z][a-z0-9+.-]*:|#|/)", re.IGNORECASE)


def _section_entries(docs_dir: Path, folder: Path) -> list[Any]:
    """Return nav entries for one folder: README first, then pages, then subfolders."""
    entries: list[Any] = []
    readme = folder / "README.md"
    if readme.is_file():
        entries.append(readme.relative_to(docs_dir).as_posix())
    for page in sorted(folder.glob("*.md")):
        if page.name != "README.md":
            entries.append(page.relative_to(docs_dir).as_posix())
    for child in sorted(p for p in folder.iterdir() if p.is_dir()):
        child_entries = _section_entries(docs_dir, child)
        if child_entries:
            entries.append({SECTION_TITLES.get(child.name, child.name): child_entries})
    return entries


def on_config(config: MkDocsConfig) -> MkDocsConfig:
    docs_dir = Path(config.docs_dir)
    nav: list[Any] = [{"Home": "index.md"}]
    for tab_title, folders in TABS:
        tab: list[Any] = []
        if tab_title == "Documentation":
            tab.append({"Overview": "README.md"})
        for name in folders:
            folder = docs_dir / name
            if not folder.is_dir():
                continue
            entries = _section_entries(docs_dir, folder)
            if len(folders) == 1:
                tab.extend(entries)
            else:
                tab.append({SECTION_TITLES.get(name, name): entries})
        nav.append({tab_title: tab})
    config.nav = nav
    return config


def on_files(files: Files, config: MkDocsConfig) -> Files:
    docs_index = files.get_file_from_path("README.md")
    if docs_index is not None:
        docs_index.dest_uri = "docs/index.html"
        docs_index.url = "docs/" if config.use_directory_urls else "docs/index.html"
        docs_index.abs_dest_path = str(Path(config.site_dir) / "docs" / "index.html")
    files.append(File.generated(config, "index.md", abs_src_path=str(LANDING_SOURCE)))
    return files


def _rewrite_target(target: str, page_dir: str, docs_dir: Path) -> str:
    bare = target[1:-1] if target.startswith("<") and target.endswith(">") else target
    if EXTERNAL.match(bare):
        return target
    path, sep, fragment = bare.partition("#")
    if not path:
        return target
    resolved = posixpath.normpath(posixpath.join(page_dir, path))
    leaves_docs = resolved == ".." or resolved.startswith("../")
    in_excluded = any(
        resolved == name or resolved.startswith(name + "/") for name in EXCLUDED_DIRS
    )
    if not (leaves_docs or in_excluded):
        return target
    repo_path = posixpath.normpath(posixpath.join("docs", resolved))
    if repo_path in (".", ""):
        return REPO_TREE.rstrip("/") + (sep + fragment if sep else "")
    is_dir = path.endswith("/") or (docs_dir.parent / repo_path).is_dir()
    base = REPO_TREE if is_dir else REPO_BLOB
    return base + repo_path + (sep + fragment if sep else "")


def on_page_markdown(
    markdown: str, page: Page, config: MkDocsConfig, files: Files
) -> str:
    if page.file.generated_by is not None:
        return markdown
    docs_dir = Path(config.docs_dir)
    page_dir = posixpath.dirname(page.file.src_uri)
    out: list[str] = []
    fence: str | None = None
    for line in markdown.splitlines(keepends=True):
        match = FENCE.match(line)
        if match:
            marker = match.group(2)
            if fence is None:
                fence = marker
            elif marker.startswith(fence):
                fence = None
            out.append(line)
            continue
        if fence is not None:
            out.append(line)
            continue
        line = INLINE_LINK.sub(
            lambda m: m.group(1) + _rewrite_target(m.group(2), page_dir, docs_dir) + m.group(3),
            line,
        )
        line = REFERENCE_LINK.sub(
            lambda m: m.group(1) + _rewrite_target(m.group(2), page_dir, docs_dir) + m.group(3),
            line,
        )
        out.append(line)
    return "".join(out)
