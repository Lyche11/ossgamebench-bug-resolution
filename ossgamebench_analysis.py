#!/usr/bin/env python3
"""
OSSGameBench Option A: Bug Resolution — Unified Analysis & Replication Script.

Single-file replication script combining the complete pipeline:
  1. extract      : extract the six main parquet tables from OSSGameBench.zip
  2. prepare      : deduplicate 20 renamed repositories (970 -> 950 projects;
                    326,182 -> 319,709 issues), identify bugs, validate links
  3. scan         : stream per-project commit parquet files (cached)
  4. build        : classify changed files (CODE / ASSET / OTHER) and build sample
  5. stats        : descriptives, Kruskal-Wallis, Mann-Whitney, cluster bootstrap,
                    stepwise regressions, paired project tests, 14 sensitivities
  6. extended     : S1-S5 stepwise models, log transform check, LOO project checks,
                    coverage funnel, and 100% complete-commit-recovery check
  7. figures      : render Figures 1-5 (PNG and PDF)
  8. final_checks : sample-selection bias & modification-size common support
                    (Tables 13-20, Figure 6)
  9. validation   : generate stratified audit samples (skips if already reviewed
                    unless --force is passed)
 10. audit_eval   : inverse-stratum-weighted & exact Clopper-Pearson audit metrics
 11. audit_review : interactive CLI to inspect or re-audit sample rows
 12. all          : run stages 1-10 end-to-end

Usage:
    python ossgamebench_analysis.py all
    python ossgamebench_analysis.py audit_review --audit classification
    python ossgamebench_analysis.py audit_review --audit bug
"""
from __future__ import annotations

import argparse
import csv
import sys
import json
import math
import os
import re
import shutil
import tempfile
import time
import warnings
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import statsmodels.formula.api as smf

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch, Rectangle
from scipy import stats

warnings.filterwarnings("ignore")

ROOT = Path(__file__).resolve().parent
DATA = Path(os.environ.get("OSS_DATA_DIR", ROOT / "data"))
ZIP_PATH = Path(os.environ.get("OSS_ZIP", ROOT / "OSSGameBench.zip"))
RESULTS = ROOT / "results"
VALIDATION = ROOT / "validation"
FIGDIR = RESULTS / "figures"

RNG_SEED = 20261119
GROUP_ORDER = ["code_only", "asset_only", "mixed"]
GROUP_LABEL = {
    "code_only": "Code-only",
    "asset_only": "Asset-only",
    "mixed": "Mixed",
    "other": "Other/Unknown",
    "unknown": "Unknown",
}
GROUP_COLOR = {
    "code_only": "#3B6EA5",
    "asset_only": "#C0504D",
    "mixed": "#7F6A93",
    "other": "#9A9A9A",
}

# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------
CODE_EXTS = {
    # C family
    ".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".hxx", ".ipp", ".inl",
    ".m", ".mm", ".cs", ".java", ".kt", ".kts", ".scala", ".groovy",
    ".py", ".pyx", ".pxd", ".rb", ".php", ".pl", ".pm", ".lua", ".toc",
    ".js", ".mjs", ".cjs", ".jsx", ".ts", ".tsx", ".dart", ".swift",
    ".go", ".rs", ".zig", ".nim", ".jl", ".r", ".rmd", ".ex", ".exs",
    ".erl", ".hrl", ".hs", ".lhs", ".clj", ".cljs", ".fs", ".fsx", ".vb",
    ".pas", ".pp", ".d", ".vala", ".v", ".sv", ".vhd", ".vhdl",
    ".f", ".f90", ".f95", ".for", ".asm", ".s", ".sql",
    ".sh", ".bash", ".zsh", ".fish", ".ps1", ".bat", ".cmd",
    # web / UI
    ".html", ".htm", ".xhtml", ".css", ".scss", ".sass", ".less", ".vue",
    ".svelte", ".astro",
    # shaders and game scripting
    ".glsl", ".hlsl", ".wgsl", ".frag", ".vert", ".geom", ".comp", ".tesc",
    ".tese", ".shader", ".gdshader", ".gd", ".gml", ".yy", ".yyp",
    ".nut", ".sqf", ".pwn", ".angelscript", ".as", ".wren", ".hx",
    ".unityshader", ".cginc", ".compute", ".metal", ".cl", ".cairo",
    ".move", ".sol", ".vy",
    # build / project code
    ".cmake", ".mk", ".gradle", ".sln", ".csproj", ".vcxproj", ".xcodeproj",
    ".pbxproj", ".pro", ".pri", ".qrc", ".ui",
    # data/schema that is executable logic
    ".proto", ".thrift",
}

ASSET_EXTS = {
    # raster / vector images
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tga", ".tif", ".tiff",
    ".webp", ".ico", ".icns", ".svg", ".psd", ".xcf", ".kra", ".aseprite",
    ".ase", ".clip", ".dds", ".ktx", ".ktx2", ".pkm", ".pvr", ".basis",
    ".exr", ".hdr", ".dib", ".pbm", ".pgm", ".ppm", ".xpm",
    # audio
    ".wav", ".mp3", ".ogg", ".oga", ".flac", ".m4a", ".aac", ".opus",
    ".mid", ".midi", ".mod", ".xm", ".it", ".s3m", ".aiff", ".aif",
    ".wma", ".caf", ".sf2", ".sfz",
    # video
    ".mp4", ".webm", ".avi", ".mkv", ".mov", ".m4v", ".ogv", ".wmv",
    # 3-D / 2-D models and animation
    ".obj", ".fbx", ".blend", ".blend1", ".gltf", ".glb", ".dae", ".3ds",
    ".stl", ".ply", ".md3", ".mdl", ".smd", ".vox", ".vta", ".dmx",
    ".x", ".lwo", ".lws", ".ma", ".mb", ".max", ".c4d", ".usd", ".usda",
    ".usdc", ".usdz", ".anim", ".controller", ".overridecontroller",
    # fonts
    ".ttf", ".otf", ".woff", ".woff2", ".eot", ".fnt", ".bmfont",
    # tile maps, atlases, sprite sheets, engine assets
    ".tmx", ".tsx", ".tsj", ".atlas", ".tpsheet", ".spriteatlas",
    ".asset", ".prefab", ".mat", ".unity", ".tscn", ".tres", ".res",
    ".scn", ".escn", ".mesh", ".physicmaterial", ".physicmaterial2d",
    ".rendertexture", ".cubemap", ".flare", ".guiskin", ".fontsettings",
    ".spriteatlasv2", ".terrainlayer", ".brush", ".gtlf",
    # game data that is primarily authored content
    ".ldtk", ".tmj", ".ogmo", ".rmmzmap", ".rxdata", ".rvdata", ".rvdata2",
    ".vmf", ".bsp", ".oramap", ".gdxui", ".skin", ".block",
    # audio/video project files
    ".aup", ".aup3", ".flp", ".als", ".logicx", ".ptx", ".rpp",
    ".prproj", ".veg", ".fcpxml",
}

OTHER_EXTS = {
    ".md", ".markdown", ".rst", ".txt", ".adoc", ".asciidoc", ".tex",
    ".pdf", ".doc", ".docx", ".odt", ".rtf", ".epub",
    ".json", ".json5", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf",
    ".properties", ".xml", ".plist", ".lock", ".csv", ".tsv", ".po", ".pot",
    ".mo", ".resx", ".strings", ".lang", ".lproj", ".gitignore",
    ".gitattributes", ".gitmodules", ".editorconfig", ".env", ".dockerignore",
    ".npmrc", ".yarnrc", ".eslintrc", ".prettierrc", ".babelrc",
    ".log", ".patch", ".diff", ".gitkeep", ".keep", ".license", ".licence",
    ".readme", ".changelog", ".authors", ".contributors", ".mailmap",
    ".dll", ".so", ".dylib", ".exe", ".bin", ".lib", ".a", ".o", ".obj",
    ".class", ".pyc", ".wasm", ".node", ".pdb", ".ilk", ".exp",
}

DOC_DIRS = {"doc", "docs", "documentation", "wiki", "screenshots", "screenshot",
            "fastlane", "changes", ".changes", "changelog.d", "dist", "build",
            "out", "target", "node_modules"}
TEST_DIRS = {"test", "tests", "testdata", "test-data", "fixtures", "__tests__",
             "spec", "specs", "integration-tests"}

CODE_BASENAMES = {
    "makefile", "gnumakefile", "cmakelists.txt", "dockerfile", "jenkinsfile",
    "build.gradle", "settings.gradle", "gradlew", "gradlew.bat", "configure",
    "meson.build", "sconstruct", "rakefile", "vagrantfile",
}

OTHER_BASENAMES = {
    "go.mod", "go.sum", "copyright", "license", "licence", "copying",
    "readme", "changelog", "authors", "contributors", "notice", "ja2_manpage",
}

ASSET_DIR_TOKENS = {
    "asset", "assets", "art", "arts", "artwork", "audio", "sound", "sounds",
    "music", "sfx", "texture", "textures", "sprite", "sprites", "model",
    "models", "mesh", "meshes", "animation", "animations", "anim", "anims",
    "font", "fonts", "shader" , "shaders", "image", "images", "img", "imgs",
    "graphic", "graphics", "gfx", "icon", "icons", "map", "maps",
    "level", "levels", "resource", "resources", "res", "media", "video",
    "videos", "cinematic", "cinematics", "voice", "voices", "dialogue",
    "portrait", "portraits", "tile", "tiles", "tileset", "tilesets",
}

ARCHIVE_EXTS = {".zip", ".tar", ".gz", ".bz2", ".xz", ".7z", ".rar", ".jar"}


def classify_path(path: str) -> str:
    """Return CODE, ASSET, OTHER, or UNKNOWN for one changed path."""
    p = str(path).strip().replace("\\", "/")
    if not p:
        return "UNKNOWN"
    low = p.lower()
    base = low.rsplit("/", 1)[-1]
    if base.startswith(".") and base.count(".") == 1:
        return "OTHER"
    suffix = ""
    if "." in base:
        suffix = "." + base.rsplit(".", 1)[-1]
    # build files whose extension would otherwise be treated as documentation
    if base in CODE_BASENAMES:
        return "CODE"
    # explicit non-asset manifest/metadata basenames (e.g., go.mod vs tracker .mod)
    if base in OTHER_BASENAMES:
        return "OTHER"
    # compound suffixes
    for comp in (".tar.gz", ".tar.bz2", ".tar.xz"):
        if low.endswith(comp):
            suffix = comp
            break
    if suffix in ARCHIVE_EXTS:
        return "OTHER"
    if suffix in CODE_EXTS:
        return "CODE"
    segments = set(re.split(r"[/_.\-]", low))
    doc_context = bool(segments & DOC_DIRS) or low.startswith("projectsettings/")
    test_context = bool(segments & TEST_DIRS)
    if suffix in ASSET_EXTS:
        if doc_context or test_context:
            return "OTHER"
        if suffix == ".asset" and ("settings" in segments or low.startswith("projectsettings/")):
            return "OTHER"
        return "ASSET"
    if suffix in OTHER_EXTS:
        return "OTHER"
    if base in CODE_BASENAMES:
        return "CODE"
    if suffix == "":
        if segments & ASSET_DIR_TOKENS:
            return "ASSET"
        return "OTHER"
    # Unknown extension: use directory context only when it is unambiguous.
    if segments & {"textures", "sprites", "audio", "sounds", "models", "meshes"}:
        return "ASSET"
    return "OTHER"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def parse_list(value) -> list:
    """Robustly parse labels/files/diff cells that may be list, ndarray or JSON."""
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    if isinstance(value, np.ndarray):
        return [v for v in value.tolist() if v is not None]
    if isinstance(value, (list, tuple, set)):
        return [v for v in value if v is not None]
    if isinstance(value, dict):
        return list(value.keys())
    if isinstance(value, str):
        s = value.strip()
        if not s or s.lower() in {"nan", "none", "null", "[]"}:
            return []
        if s[0] in "[{":
            try:
                parsed = json.loads(s)
                return parse_list(parsed)
            except Exception:
                try:
                    import ast
                    parsed = ast.literal_eval(s)
                    return parse_list(parsed)
                except Exception:
                    return [s]
        return [s]
    return [value]


def parse_labels(value) -> list[str]:
    return [str(v).strip().lower() for v in parse_list(value) if str(v).strip()]


def parse_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def norm_sha(value) -> str | None:
    s = str(value).strip().lower()
    m = re.match(r"^([0-9a-f]{40})", s)
    if m:
        return m.group(1)
    m = re.search(r"\b([0-9a-f]{40})\b", s)
    return m.group(1) if m else None


BUG_LABEL_POS = re.compile(
    r"(?<![a-z0-9])(bug|bugs|bugfix|bug[-_ ]?fix|bug report|defect|defects|"
    r"regression|crash|crash or hang|error report)(?![a-z0-9])",
    re.IGNORECASE,
)
BUG_LABEL_NEG = re.compile(
    r"(not a bug|not bug|no bug|non[- ]?bug|notabug|not-bug|feature request|"
    r"enhancement|question|discussion|suggestion|idea|documentation)",
    re.IGNORECASE,
)
NONFIX_LABEL = re.compile(
    r"^(duplicate|invalid|wontfix|won'?t fix|stale|not a bug|not bug|no bug|"
    r"non[- ]?bug|notabug|not-bug|cannot reproduce|can't reproduce|"
    r"need more info|needs more info|needs info|question|feature request|"
    r"enhancement|documentation)$",
    re.IGNORECASE,
)
TITLE_BUG = re.compile(
    r"\b(bug|crash(?:es|ed|ing)?|error|exception|broken|fails?|failure|"
    r"glitch(?:es)?|freeze(?:s|zing)?|hangs?|regression|incorrect|wrong|"
    r"doesn'?t work|does not work|not working)\b",
    re.IGNORECASE,
)
TITLE_PREFIX_BUG = re.compile(r"^\s*\[?(bug|bugfix|defect|crash|error)\]?[\s:\-]", re.IGNORECASE)
REPRO_MARKERS = re.compile(
    r"(steps? to reproduce|to reproduce|how to reproduce|expected (behavior|behaviour|result)|"
    r"actual (behavior|behaviour|result)|stack ?trace|traceback|reproduction steps|"
    r"system information|version information)",
    re.IGNORECASE,
)


def bug_label_flags(labels: list[str]) -> tuple[bool, bool]:
    joined = " | ".join(labels)
    positive = bool(BUG_LABEL_POS.search(joined)) and not bool(BUG_LABEL_NEG.search(joined))
    nonfix = any(NONFIX_LABEL.match(x) for x in labels)
    return positive, nonfix


def bug_text_flag(title: str, body: str) -> bool:
    t = "" if title is None else str(title)
    b = "" if body is None else str(body)
    if TITLE_PREFIX_BUG.search(t):
        return True
    return bool(TITLE_BUG.search(t) and REPRO_MARKERS.search(b))


# ---------------------------------------------------------------------------
# Stage 0: prepare
# ---------------------------------------------------------------------------
def ensure_dirs():
    for d in (DATA, RESULTS, FIGDIR, VALIDATION):
        d.mkdir(parents=True, exist_ok=True)


def stage_extract(force: bool = False):
    """Extract the six main parquet tables from OSSGameBench.zip."""
    ensure_dirs()
    names = ["OSSGameBench/issue.parquet", "OSSGameBench/pr.parquet",
             "OSSGameBench/pr_commit.parquet", "OSSGameBench/issue_pr.parquet",
             "OSSGameBench/project.parquet", "OSSGameBench/commit_metadata.parquet"]
    if not ZIP_PATH.exists():
        raise FileNotFoundError(f"Missing dataset archive: {ZIP_PATH}")
    z = zipfile.ZipFile(ZIP_PATH)
    for name in names:
        dst = DATA / Path(name).name
        if dst.exists() and dst.stat().st_size == z.getinfo(name).file_size and not force:
            continue
        with z.open(name) as src, open(dst, "wb") as out:
            shutil.copyfileobj(src, out, length=8 * 1024 * 1024)
    print("[extract] main tables ready in", DATA)


def load_core():
    cols_i = ["project", "number", "contributor", "title", "body", "labels",
              "state", "created_at", "closed_at"]
    cols_p = ["project", "number", "contributor", "title", "body", "labels",
              "state", "created_at", "closed_at", "merged_at"]
    iss = pd.read_parquet(DATA / "issue.parquet", columns=cols_i)
    pr = pd.read_parquet(DATA / "pr.parquet", columns=cols_p)
    ipr = pd.read_parquet(DATA / "issue_pr.parquet")
    pc = pd.read_parquet(DATA / "pr_commit.parquet")
    proj = pd.read_parquet(DATA / "project.parquet", columns=["project", "created_at", "languages", "size", "topics"])
    return iss, pr, ipr, pc, proj


def find_duplicate_projects(iss: pd.DataFrame) -> set[str]:
    """Detect exact duplicate renamed/transferred repositories by identical earliest issue signature."""
    first = (iss.sort_values(["project", "number"])
                .drop_duplicates("project")[["project", "number", "title", "created_at"]])
    first["sig"] = (first["number"].astype(str) + "|" +
                    first["created_at"].astype(str) + "|" +
                    first["title"].astype(str))
    dups = first[first.duplicated("sig", keep=False)].sort_values(["sig", "project"])
    drop = set()
    for _, grp in dups.groupby("sig"):
        projs = sorted(grp.project.tolist())
        # Keep the first sorted slug deterministically; drop the duplicate alias(es).
        drop.update(projs[1:])
    return drop


def stage_prepare(force: bool = False):
    ensure_dirs()
    required = ["issue.parquet", "pr.parquet", "issue_pr.parquet",
                "pr_commit.parquet", "project.parquet"]
    if any(not (DATA / f).exists() for f in required):
        stage_extract()
    t0 = time.time()
    iss, pr, ipr, pc, proj = load_core()
    qa = {}

    qa["raw_archive_projects"] = int(len(proj))
    qa["raw_archive_issues"] = int(len(iss))
    dup_projects = find_duplicate_projects(iss)
    qa["duplicate_renamed_projects_dropped"] = int(len(dup_projects))
    if dup_projects:
        iss = iss[~iss.project.isin(dup_projects)].copy()
        pr = pr[~pr.project.isin(dup_projects)].copy()
        ipr = ipr[~ipr.project.isin(dup_projects)].copy()
        pc = pc[~pc.project.isin(dup_projects)].copy()
        proj = proj[~proj.project.isin(dup_projects)].copy()

    qa["dedup_projects"] = int(len(proj))
    qa["raw_issues"] = len(iss)
    qa["raw_prs"] = len(pr)
    qa["raw_issue_pr_links"] = len(ipr)
    qa["raw_pr_commit_links"] = len(pc)

    for df in (iss, pr):
        df["created_at"] = parse_dt(df["created_at"])
        df["closed_at"] = parse_dt(df["closed_at"])
    pr["merged_at"] = parse_dt(pr["merged_at"])
    proj["created_at"] = parse_dt(proj["created_at"])

    # GitHub's issue search sometimes returns PRs; remove exact PR numbers.
    pr_keys = set(zip(pr.project, pr.number))
    iss["is_pr_record"] = [(p, n) in pr_keys for p, n in zip(iss.project, iss.number)]
    qa["issue_records_that_are_prs"] = int(iss.is_pr_record.sum())
    iss = iss[~iss.is_pr_record].copy()

    iss["labels_list"] = iss.labels.map(parse_labels)
    flags = iss.labels_list.map(bug_label_flags)
    iss["bug_label"] = [x[0] for x in flags]
    iss["nonfix_label"] = [x[1] for x in flags]
    iss["bug_text"] = [bug_text_flag(t, b) for t, b in zip(iss.title, iss.body)]
    iss["bug_any"] = iss.bug_label | iss.bug_text
    iss["created_year"] = iss.created_at.dt.year
    iss["duration_days"] = (iss.closed_at - iss.created_at).dt.total_seconds() / 86400.0

    qa["closed_issues"] = int((iss.state == "CLOSED").sum())
    qa["open_issues"] = int((iss.state == "OPEN").sum())
    qa["missing_closed_at"] = int(iss.closed_at.isna().sum())
    qa["negative_duration"] = int((iss.duration_days < 0).sum())
    qa["zero_duration"] = int((iss.duration_days == 0).sum())
    qa["bug_label_issues"] = int(iss.bug_label.sum())
    qa["bug_text_issues"] = int(iss.bug_text.sum())
    qa["bug_any_issues"] = int(iss.bug_any.sum())

    base = iss[(iss.state == "CLOSED") & iss.closed_at.notna() & iss.created_at.notna()].copy()
    base = base[base.duration_days >= 0].copy()
    qa["analysis_eligible_closed_issues"] = len(base)
    qa["analysis_eligible_bug_label"] = int(base.bug_label.sum())
    qa["analysis_eligible_bug_any"] = int(base.bug_any.sum())

    # --- issue-PR links -----------------------------------------------------
    ipr = ipr.drop_duplicates(subset=["project", "issue_number", "pr_number"]).copy()
    qa["unique_issue_pr_links"] = len(ipr)
    pr_small = pr[["project", "number", "state", "created_at", "closed_at", "merged_at"]].rename(
        columns={"number": "pr_number", "created_at": "pr_created_at",
                 "closed_at": "pr_closed_at", "merged_at": "pr_merged_at",
                 "state": "pr_state"})
    links = ipr.merge(pr_small, on=["project", "pr_number"], how="left", validate="many_to_one")
    qa["links_to_missing_pr"] = int(links.pr_state.isna().sum())

    ncommit = (pc.groupby(["project", "pr_number"]).size()
                 .rename("pr_commit_count").reset_index())
    links = links.merge(ncommit, on=["project", "pr_number"], how="left")
    links["pr_commit_count"] = links.pr_commit_count.fillna(0).astype(int)
    links["pr_merged"] = links.pr_state.eq("MERGED") | links.pr_merged_at.notna()
    links["pr_has_commits"] = links.pr_commit_count > 0

    issue_keys = base[["project", "number"]].rename(columns={"number": "issue_number"})
    issue_keys["in_issue_table"] = True
    links = links.merge(issue_keys, on=["project", "issue_number"], how="left")
    links["linked_issue_in_table"] = links.in_issue_table.fillna(False).astype(bool)
    links = links.drop(columns=["in_issue_table"])
    qa["links_to_missing_issue"] = int((~links.linked_issue_in_table).sum())

    # Attach issue metadata (bug flags and outcome) for links that are in base.
    issue_meta = base[["project", "number", "bug_label", "bug_text", "bug_any",
                       "nonfix_label", "duration_days", "created_at", "closed_at"]].rename(
        columns={"number": "issue_number", "created_at": "issue_created_at",
                 "closed_at": "issue_closed_at"})
    links = links.merge(issue_meta, on=["project", "issue_number"], how="left")

    # Only links whose issue is a closed, non-PR issue in the benchmark.
    valid_links = links[links.linked_issue_in_table].copy()
    valid_links["nonfix_label"] = valid_links.nonfix_label.fillna(False).astype(bool)

    # Per-issue linkage summary over all valid links (merged or not).
    agg = valid_links.groupby(["project", "issue_number"]).agg(
        n_linked_prs=("pr_number", "nunique"),
        n_merged_prs=("pr_merged", "sum"),
        n_linked_commits=("pr_commit_count", "sum"),
        any_commit=("pr_has_commits", "any"),
        pr_numbers=("pr_number", lambda s: ";".join(str(int(x)) for x in sorted(set(s)))),
    ).reset_index()
    for c in ["n_merged_prs", "n_linked_commits"]:
        agg[c] = agg[c].astype(int)

    # Relevant hashes: bug_any issues with a merged PR and at least one commit.
    rel = valid_links[(valid_links.bug_any == True) & valid_links.pr_merged & valid_links.pr_has_commits]
    rel_shas = rel[["project", "pr_number", "issue_number"]].merge(
        pc, on=["project", "pr_number"], how="inner")
    rel_shas["sha"] = rel_shas.commit_sha.map(norm_sha)
    rel_shas = rel_shas.dropna(subset=["sha"]).drop_duplicates(
        subset=["project", "sha", "issue_number", "pr_number"])

    base = base.merge(agg, left_on=["project", "number"],
                      right_on=["project", "issue_number"], how="left")
    base = base.drop(columns=["issue_number"])
    for c in ["n_linked_prs", "n_merged_prs", "n_linked_commits"]:
        base[c] = base[c].fillna(0).astype(int)
    base["any_commit"] = base["any_commit"].fillna(False).astype(bool)

    base_out = base[["project", "number", "contributor", "title", "labels_list",
                     "state", "created_at", "closed_at", "created_year",
                     "duration_days", "bug_label", "bug_text", "bug_any",
                     "nonfix_label", "n_linked_prs", "n_merged_prs",
                     "n_linked_commits", "any_commit", "pr_numbers"]].copy()
    base_out["labels"] = base_out.labels_list.map(lambda x: "|".join(x))
    base_out = base_out.drop(columns=["labels_list"])
    base_out.to_parquet(RESULTS / "analysis_base.parquet", index=False)

    valid_links.to_parquet(RESULTS / "bug_issue_links.parquet", index=False)
    rel_shas[["project", "pr_number", "issue_number", "sha"]].to_parquet(
        RESULTS / "relevant_shas.parquet", index=False)

    qa["valid_issue_pr_links"] = len(valid_links)
    qa["links_bug_any_merged_with_commits"] = len(rel)
    qa["unique_relevant_commits"] = rel_shas.sha.nunique()
    qa["projects_with_relevant_commits"] = rel_shas.project.nunique()
    qa["bug_any_closed_with_merged_pr"] = rel[["project", "issue_number"]].drop_duplicates().shape[0]

    # linkage coverage table by project
    bug = base[base.bug_any].copy()
    bug["has_merged_pr"] = bug.n_merged_prs > 0
    cov = bug.groupby("project").agg(
        bug_issues=("number", "size"),
        with_merged_pr=("has_merged_pr", "sum"),
        median_closure_days=("duration_days", "median"),
    ).reset_index()
    cov["coverage"] = cov.with_merged_pr / cov.bug_issues
    cov.sort_values(["coverage", "bug_issues"], ascending=[True, False]).to_csv(
        VALIDATION / "linkage_coverage.csv", index=False)

    # linked vs unlinked bug comparison (selection bias)
    bug["linked"] = bug.n_merged_prs > 0
    bias_rows = []
    for flag, name in [(True, "linked"), (False, "unlinked")]:
        sub = bug[bug.linked == flag]
        bias_rows.append({
            "subset": name, "n": len(sub),
            "median_closure_days": sub.duration_days.median(),
            "mean_closure_days": sub.duration_days.mean(),
            "median_created_year": sub.created_year.median(),
            "pct_nonfix_label": 100 * sub.nonfix_label.mean(),
        })
    pd.DataFrame(bias_rows).to_csv(RESULTS / "table_linkage_bias.csv", index=False)

    with open(VALIDATION / "data_quality_checks.json", "w", encoding="utf-8") as f:
        json.dump(qa, f, indent=2)
    pd.DataFrame([{"check": k, "value": v} for k, v in qa.items()]).to_csv(
        VALIDATION / "data_quality_checks.csv", index=False)

    # bug label examples for rule documentation
    ex = base[base.bug_label].sample(min(40, int(base.bug_label.sum())), random_state=RNG_SEED)
    ex[["project", "number", "title", "labels", "duration_days"]].to_csv(
        VALIDATION / "bug_label_examples.csv", index=False)

    print(f"[prepare] {time.time()-t0:.1f}s issues={len(base)} "
          f"bug_any={int(base.bug_any.sum())} links={len(valid_links)} "
          f"relevant_commits={rel_shas.sha.nunique()}")


# ---------------------------------------------------------------------------
# Stage 1: scan commit parquet files
# ---------------------------------------------------------------------------
def project_commit_files(z: zipfile.ZipFile) -> dict[str, list[str]]:
    """Map project -> all commit parquet entries (handles split files)."""
    mapping = defaultdict(list)
    for name in z.namelist():
        if not (name.startswith("OSSGameBench/commits/") and name.endswith(".parquet")):
            continue
        stem = name.split("/")[-1][:-len(".parquet")]
        if not stem.startswith("commits|"):
            continue
        body = stem[len("commits|"):]
        parts = body.split("|")
        if len(parts) < 2:
            continue
        # last part may be a split index (1,2,...) or a repository name
        proj = parts[0] + "/" + parts[1]
        mapping[proj].append(name)
    return mapping


def stage_scan(force: bool = False, limit_projects: int | None = None):
    ensure_dirs()
    out_path = RESULTS / "commit_files_raw.parquet"
    log_path = RESULTS / "commit_scan_log.csv"
    if out_path.exists() and not force:
        print("[scan] cached, skipping")
        return
    t0 = time.time()
    rel = pd.read_parquet(RESULTS / "relevant_shas.parquet")
    rel["sha"] = rel.sha.map(norm_sha)
    rel = rel.dropna(subset=["sha"])
    by_project = rel.groupby("project").sha.apply(lambda s: set(s)).to_dict()
    projects = sorted(by_project)
    if limit_projects:
        projects = projects[:limit_projects]

    z = zipfile.ZipFile(ZIP_PATH)
    mapping = project_commit_files(z)
    missing_project = [p for p in projects if p not in mapping]

    all_rows = []
    logs = []
    tmp_dir = DATA / "_scan_tmp"
    tmp_dir.mkdir(exist_ok=True)
    for i, proj in enumerate(projects, 1):
        wanted = by_project[proj]
        names = mapping.get(proj, [])
        matched = 0
        rows = 0
        files_kept = 0
        t1 = time.time()
        for name in names:
            tmp = tmp_dir / "current.parquet"
            with z.open(name) as src, open(tmp, "wb") as dst:
                shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
            pf = pq.ParquetFile(tmp)
            for batch in pf.iter_batches(batch_size=100_000, columns=["sha", "files"]):
                shas = batch.column("sha").to_pylist()
                files_col = batch.column("files").to_pylist()
                rows += len(shas)
                for sha_raw, files in zip(shas, files_col):
                    sha = norm_sha(sha_raw)
                    if sha is None or sha not in wanted:
                        continue
                    matched += 1
                    for f in parse_list(files):
                        all_rows.append((proj, sha, str(f)))
                        files_kept += 1
            del pf
            tmp.unlink(missing_ok=True)
        logs.append({"project": proj, "commit_files": len(names),
                     "commit_rows": rows, "matched_commits": matched,
                     "matched_file_rows": files_kept,
                     "wanted_commits": len(wanted),
                     "seconds": round(time.time() - t1, 1)})
        if i % 10 == 0 or i == len(projects):
            print(f"[scan] {i}/{len(projects)} {proj} rows={rows} matched={matched} "
                  f"elapsed={time.time()-t0:.0f}s", flush=True)
            pd.DataFrame(all_rows, columns=["project", "sha", "file"]).to_parquet(
                out_path, index=False)
            pd.DataFrame(logs).to_csv(log_path, index=False)

    df = pd.DataFrame(all_rows, columns=["project", "sha", "file"])
    df.drop_duplicates().to_parquet(out_path, index=False)
    pd.DataFrame(logs).to_csv(log_path, index=False)
    print(f"[scan] done rows={len(df)} projects={len(projects)} "
          f"missing_project_files={len(missing_project)} {time.time()-t0:.0f}s")


# ---------------------------------------------------------------------------
# Stage 2: build issue-level classification
# ---------------------------------------------------------------------------
def classify_file_set(files) -> tuple[str, int, int, int, int]:
    if files is None:
        fs = set()
    elif isinstance(files, float) and math.isnan(files):
        fs = set()
    elif isinstance(files, str):
        fs = {files} if files.strip() else set()
    else:
        try:
            fs = {str(f) for f in files if f is not None and str(f).strip()}
        except TypeError:
            fs = set()
    classes = [classify_path(f) for f in fs]
    nc = classes.count("CODE")
    na = classes.count("ASSET")
    no = len(classes) - nc - na
    if not fs:
        g = "unknown"
    elif nc and na:
        g = "mixed"
    elif nc:
        g = "code_only"
    elif na:
        g = "asset_only"
    else:
        g = "other"
    return g, len(fs), nc, na, no


def _union_files(series) -> set:
    out = set()
    for t in series.dropna():
        if isinstance(t, (list, tuple, set, np.ndarray)):
            out.update(str(x) for x in t)
        else:
            out.add(str(t))
    return out


def stage_build(force: bool = False):
    ensure_dirs()
    t0 = time.time()
    base = pd.read_parquet(RESULTS / "analysis_base.parquet")
    links = pd.read_parquet(RESULTS / "bug_issue_links.parquet")
    rel = pd.read_parquet(RESULTS / "relevant_shas.parquet")
    cf = pd.read_parquet(RESULTS / "commit_files_raw.parquet")

    cf["sha"] = cf.sha.map(norm_sha)
    cf = cf.dropna(subset=["sha"]).drop_duplicates(subset=["project", "sha", "file"])
    cf["file_class"] = cf.file.map(classify_path)

    prf = rel.merge(cf, on=["project", "sha"], how="left")
    prf["file_class"] = prf.file_class.fillna("MISSING")
    found = prf.dropna(subset=["file"]).drop_duplicates(
        subset=["project", "pr_number", "file"])
    n_files = found.groupby(["project", "pr_number"]).size().rename("n_files")
    class_counts = (found.groupby(["project", "pr_number", "file_class"]).size()
                    .unstack(fill_value=0))
    for col in ("CODE", "ASSET", "OTHER"):
        if col not in class_counts:
            class_counts[col] = 0
    files_by_pr = (found.groupby(["project", "pr_number"]).file
                   .apply(lambda s: tuple(sorted(set(s)))).rename("files"))

    pr_agg = pd.concat([n_files, class_counts[["CODE", "ASSET", "OTHER"]],
                        files_by_pr], axis=1).reset_index()
    pr_agg.columns = ["project", "pr_number", "n_files", "n_code", "n_asset",
                      "n_other", "files"]

    bug_links = links[(links.bug_any == True) & (links.linked_issue_in_table == True)].copy()
    merged = bug_links[bug_links.pr_merged & bug_links.pr_has_commits].copy()
    merged = merged.merge(pr_agg, on=["project", "pr_number"], how="left")
    for c in ("n_files", "n_code", "n_asset", "n_other"):
        merged[c] = merged[c].fillna(0).astype(int)
    merged["pr_merged_at"] = parse_dt(merged.pr_merged_at)

    # Union of all merged linked PRs (primary rule).
    union = merged.groupby(["project", "issue_number"]).agg(
        files=("files", lambda s: tuple(_union_files(s))),
        n_prs_with_files=("n_files", lambda s: int((s > 0).sum())),
        n_prs_merged=("pr_number", "nunique"),
        n_commits=("pr_commit_count", "sum"),
        first_merge_at=("pr_merged_at", "min"),
        pr_numbers_files=("pr_number", lambda s: ";".join(str(int(x)) for x in sorted(set(s)))),
    ).reset_index()

    rows = []
    for rec in union.itertuples(index=False):
        g, nf, nc, na, no = classify_file_set(rec.files)
        rows.append((rec.project, rec.issue_number, g, nf, nc, na, no,
                     rec.n_prs_with_files, rec.n_prs_merged, int(rec.n_commits),
                     rec.first_merge_at, rec.pr_numbers_files, rec.files))
    issue_all = pd.DataFrame(rows, columns=[
        "project", "issue_number", "group", "n_files", "n_code", "n_asset",
        "n_other", "n_prs_with_files", "n_prs_merged", "n_commits",
        "first_merge_at", "pr_numbers_files", "files_union"])

    # Sensitivity: largest PR and earliest merged PR.
    largest = (merged.sort_values(["n_files", "pr_merged_at"],
                                  ascending=[False, True])
               .drop_duplicates(["project", "issue_number"]))
    largest["group_largest"] = largest.files.map(lambda x: classify_file_set(x)[0])
    earliest = (merged.sort_values(["pr_merged_at", "n_files"],
                                   ascending=[True, False])
                .drop_duplicates(["project", "issue_number"]))
    earliest["group_earliest"] = earliest.files.map(lambda x: classify_file_set(x)[0])

    ds = base[base.bug_any].copy()
    ds = ds.merge(issue_all, left_on=["project", "number"],
                  right_on=["project", "issue_number"], how="left")
    ds = ds.drop(columns=["issue_number"])
    ds = ds.merge(largest[["project", "issue_number", "group_largest"]],
                  left_on=["project", "number"], right_on=["project", "issue_number"], how="left")
    ds = ds.drop(columns=["issue_number"])
    ds = ds.merge(earliest[["project", "issue_number", "group_earliest"]],
                  left_on=["project", "number"], right_on=["project", "issue_number"], how="left")
    ds = ds.drop(columns=["issue_number"])
    ds["group"] = ds.group.fillna("unknown")
    ds["group_largest"] = ds.group_largest.fillna("unknown")
    ds["group_earliest"] = ds.group_earliest.fillna("unknown")
    for c in ("n_files", "n_code", "n_asset", "n_other", "n_prs_with_files",
              "n_prs_merged", "n_commits"):
        ds[c] = ds[c].fillna(0).astype(int)
    ds["first_merge_at"] = parse_dt(ds.first_merge_at)
    ds["merge_lag_days"] = (ds.first_merge_at - ds.created_at).dt.total_seconds() / 86400
    ds["closed_after_merge"] = (ds.closed_at >= ds.first_merge_at)
    ds.loc[ds.first_merge_at.isna(), "closed_after_merge"] = False
    ds["asset_related"] = ds.group.isin(["asset_only", "mixed"])
    ds["file_examples"] = ds.files_union.map(
        lambda x: ";".join(list(x)[:12]) if isinstance(x, (tuple, list, set)) else "")

    cols = ["project", "number", "contributor", "title", "labels", "created_at",
            "closed_at", "created_year", "duration_days", "bug_label", "bug_text",
            "bug_any", "nonfix_label", "n_linked_prs", "n_merged_prs",
            "n_linked_commits", "n_prs_merged", "n_prs_with_files", "n_commits",
            "n_files", "n_code", "n_asset", "n_other", "group", "group_largest",
            "group_earliest", "asset_related", "first_merge_at", "merge_lag_days",
            "closed_after_merge", "pr_numbers_files", "file_examples"]
    ds = ds[cols].rename(columns={"number": "issue_number"})
    ds.to_csv(RESULTS / "analysis_dataset.csv", index=False)
    known = ds[ds.group.isin(GROUP_ORDER)].copy()
    known.to_csv(RESULTS / "analysis_sample.csv", index=False)

    qa_path = VALIDATION / "data_quality_checks.json"
    qa_prev = json.load(open(qa_path, encoding="utf-8")) if qa_path.exists() else {}
    total_dedup_issues = int(qa_prev.get("raw_issues", 319709))
    flow = [
        ("All issue records in OSSGameBench", total_dedup_issues),
        ("Closed non-PR issues with valid timestamps", len(base)),
        ("Bug reports by label or conservative text rule", int(ds.shape[0])),
        ("  - by issue label", int(ds.bug_label.sum())),
        ("  - by text rule only", int((ds.bug_text & ~ds.bug_label).sum())),
        ("Linked to >=1 merged PR", int((ds.n_merged_prs > 0).sum())),
        ("At least one changed file recovered", int((ds.n_files > 0).sum())),
        ("Classified as code-only", int((ds.group == "code_only").sum())),
        ("Classified as asset-only", int((ds.group == "asset_only").sum())),
        ("Classified as mixed", int((ds.group == "mixed").sum())),
        ("Other (docs/config only)", int((ds.group == "other").sum())),
        ("Unknown / no file data", int((ds.group == "unknown").sum())),
        ("Final known-group analysis sample", len(known)),
        ("  - excluded: no merged linked PR", int((ds.n_merged_prs == 0).sum())),
        ("  - excluded: merged PR but no recovered files",
         int(((ds.n_merged_prs > 0) & (ds.n_files == 0)).sum())),
    ]
    pd.DataFrame(flow, columns=["stage", "n"]).to_csv(
        RESULTS / "table_sample_flow.csv", index=False)

    qa_path = VALIDATION / "data_quality_checks.json"
    qa = json.load(open(qa_path, encoding="utf-8")) if qa_path.exists() else {}
    qa.update({
        "scan_relevant_commits_found": int(rel.merge(
            cf[["project", "sha"]].drop_duplicates(), on=["project", "sha"]).sha.nunique()),
        "scan_file_rows": int(len(cf)),
        "issues_with_recovered_files": int((ds.n_files > 0).sum()),
        "final_code_only": int((ds.group == "code_only").sum()),
        "final_asset_only": int((ds.group == "asset_only").sum()),
        "final_mixed": int((ds.group == "mixed").sum()),
        "final_other": int((ds.group == "other").sum()),
        "final_unknown": int((ds.group == "unknown").sum()),
        "final_known_sample": int(len(known)),
        "final_projects": int(known.project.nunique()),
    })
    json.dump(qa, open(qa_path, "w", encoding="utf-8"), indent=2)
    pd.DataFrame([{"check": k, "value": v} for k, v in qa.items()]).to_csv(
        VALIDATION / "data_quality_checks.csv", index=False)

    summ = (known.groupby(["project", "group"]).duration_days
            .agg(["size", "median"]).reset_index())
    summ.to_csv(RESULTS / "project_group_summary.csv", index=False)
    print(f"[build] {time.time()-t0:.1f}s known={len(known)} "
          f"groups={known.group.value_counts().to_dict()}")
    return ds


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------
def cluster_bootstrap(df: pd.DataFrame, B: int = 2000, seed: int = RNG_SEED):
    """Cluster bootstrap by project. Returns group medians and median differences."""
    d = df[df.group.isin(GROUP_ORDER)].copy()
    y = d.duration_days.to_numpy(float)
    g = d.group.to_numpy()
    p = d.project.to_numpy()
    codes, uniques = pd.factorize(p)
    idx_by = [np.flatnonzero(codes == i) for i in range(len(uniques))]
    rng = np.random.default_rng(seed)
    out = {k: [] for k in ["code_only", "asset_only", "mixed", "asset_related"]}
    diffs = {k: [] for k in ["asset_minus_code", "mixed_minus_code",
                             "mixed_minus_asset", "assetrel_minus_code"]}
    for _ in range(B):
        pick = rng.integers(0, len(uniques), len(uniques))
        idx = np.concatenate([idx_by[i] for i in pick])
        gg = g[idx]
        yy = y[idx]
        vals = {}
        for name, mask in [("code_only", gg == "code_only"),
                           ("asset_only", gg == "asset_only"),
                           ("mixed", gg == "mixed"),
                           ("asset_related", (gg == "asset_only") | (gg == "mixed"))]:
            vals[name] = np.median(yy[mask]) if mask.any() else np.nan
            out[name].append(vals[name])
        diffs["asset_minus_code"].append(vals["asset_only"] - vals["code_only"])
        diffs["mixed_minus_code"].append(vals["mixed"] - vals["code_only"])
        diffs["mixed_minus_asset"].append(vals["mixed"] - vals["asset_only"])
        diffs["assetrel_minus_code"].append(vals["asset_related"] - vals["code_only"])
    return out, diffs


def ci95(a):
    a = np.asarray(a, float)
    a = a[~np.isnan(a)]
    if len(a) == 0:
        return (np.nan, np.nan)
    return tuple(np.percentile(a, [2.5, 97.5]))


def desc_row(x: pd.Series) -> dict:
    x = x.dropna().to_numpy(float)
    if len(x) == 0:
        return {}
    q1, q3 = np.percentile(x, [25, 75])
    return {"n": len(x), "mean": x.mean(), "sd": x.std(ddof=1) if len(x) > 1 else np.nan,
            "median": np.median(x), "q1": q1, "q3": q3, "iqr": q3 - q1,
            "p90": np.percentile(x, 90), "p95": np.percentile(x, 95),
            "min": x.min(), "max": x.max()}


def fit_log_model(df: pd.DataFrame, fe: bool = False, assetrel: bool = False,
                  outcome: str = "y", controls: bool = True):
    d = df.copy()
    if "size_log" not in d:
        d["size_log"] = np.log1p(d.n_files.clip(lower=1))
    if outcome not in d:
        d[outcome] = np.log1p(d.duration_days)
    if assetrel:
        d["asset_related_num"] = d.asset_related.astype(int)
        rhs = "asset_related_num"
    else:
        rhs = "C(group, Treatment('code_only'))"
    if controls:
        rhs += " + size_log + C(created_year)"
    if fe:
        rhs += " + C(project)"
    formula = f"{outcome} ~ {rhs}"
    mod = smf.ols(formula, data=d)
    res = mod.fit(cov_type="cluster", cov_kwds={"groups": d.project})
    params = res.params
    bse = res.bse
    pvals = res.pvalues
    conf = res.conf_int()
    out = {}
    for term in params.index:
        key = None
        if "asset_related_num" in term:
            key = "asset_related"
        elif "asset_only" in term:
            key = "asset_only"
        elif "mixed" in term:
            key = "mixed"
        if key:
            out[key] = {
                "beta": params[term], "se": bse[term], "p": pvals[term],
                "ci_low": conf.loc[term, 0], "ci_high": conf.loc[term, 1],
                "ratio": math.exp(params[term]),
                "ratio_low": math.exp(conf.loc[term, 0]),
                "ratio_high": math.exp(conf.loc[term, 1]),
            }
    out["_meta"] = {"n": int(res.nobs), "projects": int(d.project.nunique()),
                    "r2": res.rsquared, "r2_adj": res.rsquared_adj,
                    "df_model": res.df_model, "df_resid": res.df_resid}
    return res, out


def paired_project_stats(df: pd.DataFrame, other: str, label: str) -> tuple[dict, pd.DataFrame]:
    sub = df[df.group.isin(["code_only", other])]
    piv = (sub.groupby(["project", "group"]).duration_days.median().unstack())
    if "code_only" not in piv or other not in piv:
        return {}, pd.DataFrame()
    n = sub.groupby(["project", "group"]).size().unstack(fill_value=0)
    keep = (n.get("code_only", 0) >= 3) & (n.get(other, 0) >= 3)
    piv = piv[keep.reindex(piv.index).fillna(False)]
    n = n[keep.reindex(n.index).fillna(False)]
    piv = piv[["code_only", other]].dropna()
    if len(piv) < 2:
        return {}, pd.DataFrame()
    diff = piv[other] - piv["code_only"]
    try:
        w = stats.wilcoxon(piv[other], piv["code_only"], zero_method="wilcox",
                           alternative="two-sided")
        wp = float(w.pvalue)
    except Exception:
        wp = np.nan
    pos = int((diff > 0).sum())
    neg = int((diff < 0).sum())
    try:
        sign_p = float(stats.binomtest(pos, pos + neg, 0.5).pvalue)
    except Exception:
        sign_p = np.nan
    row = {"comparison": label, "projects": len(piv),
           "median_project_median_diff_days": float(diff.median()),
           "q1": float(diff.quantile(.25)), "q3": float(diff.quantile(.75)),
           "projects_positive": pos, "projects_negative": neg,
           "wilcoxon_p": wp, "sign_test_p": sign_p}
    dat = pd.DataFrame({"project": piv.index, "code_only": piv["code_only"],
                        other: piv[other], "diff": diff,
                        "n_code": n.get("code_only", 0).values,
                        "n_other": n.get(other, 0).values})
    return row, dat


def stage_stats(force: bool = False):
    ensure_dirs()
    t0 = time.time()
    ds = pd.read_csv(RESULTS / "analysis_dataset.csv")
    df = ds[ds.group.isin(GROUP_ORDER)].copy()
    df["group"] = pd.Categorical(df.group, categories=GROUP_ORDER)
    df["asset_related"] = df.asset_related.astype(bool)
    df["duration_days"] = pd.to_numeric(df.duration_days, errors="coerce")
    df = df[df.duration_days.notna() & (df.duration_days >= 0)].copy()
    df["n_files"] = df.n_files.clip(lower=1)
    df["size_log"] = np.log1p(df.n_files)
    df["log_days"] = np.log1p(df.duration_days)

    # Descriptives
    rows = []
    for g in GROUP_ORDER:
        sub = df[df.group == g]
        r = desc_row(sub.duration_days)
        r.update({"group": g, "label": GROUP_LABEL[g],
                  "pct": 100 * len(sub) / len(df)})
        rows.append(r)
    ar = desc_row(df[df.asset_related].duration_days)
    ar.update({"group": "asset_related", "label": "Asset-related (asset-only + mixed)",
               "pct": 100 * df.asset_related.mean()})
    rows.append(ar)
    allr = desc_row(df.duration_days)
    allr.update({"group": "all", "label": "All known-group bug reports", "pct": 100})
    rows.append(allr)
    desc = pd.DataFrame(rows)
    desc.to_csv(RESULTS / "table1_descriptives.csv", index=False)

    # Omnibus + pairwise tests
    groups = [df[df.group == g].duration_days.to_numpy(float) for g in GROUP_ORDER]
    H, p_kw = stats.kruskal(*groups)
    test_rows = []
    pairs = [("code_only", "asset_only"), ("code_only", "mixed"),
             ("asset_only", "mixed")]
    for a, b in pairs:
        xa = df[df.group == a].duration_days.to_numpy(float)
        xb = df[df.group == b].duration_days.to_numpy(float)
        U, p = stats.mannwhitneyu(xa, xb, alternative="two-sided", method="asymptotic")
        delta = 2 * U / (len(xa) * len(xb)) - 1
        test_rows.append({"comparison": f"{a} vs {b}", "n_a": len(xa), "n_b": len(xb),
                          "median_a": np.median(xa), "median_b": np.median(xb),
                          "median_diff_a_minus_b": np.median(xa) - np.median(xb),
                          "cliffs_delta": delta, "U": U, "p_value": p})
    xa = df[df.asset_related].duration_days.to_numpy(float)
    xb = df[df.group == "code_only"].duration_days.to_numpy(float)
    U, p = stats.mannwhitneyu(xa, xb, alternative="two-sided", method="asymptotic")
    test_rows.append({"comparison": "asset_related vs code_only", "n_a": len(xa),
                      "n_b": len(xb), "median_a": np.median(xa),
                      "median_b": np.median(xb),
                      "median_diff_a_minus_b": np.median(xa) - np.median(xb),
                      "cliffs_delta": 2 * U / (len(xa) * len(xb)) - 1,
                      "U": U, "p_value": p})
    tests = pd.DataFrame(test_rows)
    from statsmodels.stats.multitest import multipletests
    mask = tests.comparison != "asset_related vs code_only"
    tests.loc[mask, "p_adj_bh"] = multipletests(tests.loc[mask, "p_value"], method="fdr_bh")[1]
    tests.attrs = {"kruskal_H": H, "kruskal_p": p_kw}
    tests.to_csv(RESULTS / "table2_pairwise_tests.csv", index=False)
    with open(RESULTS / "kruskal_wallis.txt", "w", encoding="utf-8") as f:
        f.write(f"Kruskal-Wallis H={H:.4f}, p={p_kw:.6g}, groups={GROUP_ORDER}\n")

    # Cluster bootstrap
    boot, diffs = cluster_bootstrap(df, B=2000)
    boot_rows = []
    for g in ["code_only", "asset_only", "mixed", "asset_related"]:
        lo, hi = ci95(boot[g])
        boot_rows.append({"quantity": f"median_{g}", "estimate": float(np.nanmedian(boot[g])),
                          "ci_low": lo, "ci_high": hi})
    for k, v in diffs.items():
        lo, hi = ci95(v)
        boot_rows.append({"quantity": k, "estimate": float(np.nanmedian(v)),
                          "ci_low": lo, "ci_high": hi})
    pd.DataFrame(boot_rows).to_csv(RESULTS / "table_bootstrap.csv", index=False)
    np.savez_compressed(RESULTS / "bootstrap_replicates.npz",
                        **{f"g_{k}": np.array(v) for k, v in boot.items()},
                        **{f"d_{k}": np.array(v) for k, v in diffs.items()})

    # Add bootstrap CI to pairwise table (asset-code, mixed-code).
    ci_map = {"code_only vs asset_only": "asset_minus_code",
              "code_only vs mixed": "mixed_minus_code",
              "asset_only vs mixed": "mixed_minus_asset"}
    # Align CI direction with median_diff_a_minus_b (first group minus second).
    for i, row in tests.iterrows():
        key = ci_map.get(row.comparison)
        if key:
            lo, hi = ci95(diffs[key])
            tests.loc[i, "median_diff_ci_low"] = -hi
            tests.loc[i, "median_diff_ci_high"] = -lo
    tests.to_csv(RESULTS / "table2_pairwise_tests.csv", index=False)

    # Regressions
    reg_rows = []
    diag_lines = [f"Sample n={len(df)}, projects={df.project.nunique()}"]
    for name, fe, controls in [("M1: group only", False, False),
                               ("M2: + size + creation year", False, True),
                               ("M3: + project fixed effects", True, True)]:
        res, out = fit_log_model(df, fe=fe, controls=controls)
        for term in ("asset_only", "mixed"):
            if term in out:
                o = out[term]
                reg_rows.append({"model": name, "term": term, "beta": o["beta"],
                                 "se": o["se"], "p_value": o["p"],
                                 "ratio_days_plus1": o["ratio"],
                                 "ratio_ci_low": o["ratio_low"],
                                 "ratio_ci_high": o["ratio_high"],
                                 "n": out["_meta"]["n"],
                                 "projects": out["_meta"]["projects"],
                                 "r2": out["_meta"]["r2"]})
        diag_lines.append(f"{name}: n={out['_meta']['n']}, R2={out['_meta']['r2']:.4f}, "
                          f"adjR2={out['_meta']['r2_adj']:.4f}")
    res, out = fit_log_model(df, fe=True, assetrel=True)
    if "asset_related" in out:
        o = out["asset_related"]
        reg_rows.append({"model": "M4: asset-related + project FE", "term": "asset_related",
                         "beta": o["beta"], "se": o["se"], "p_value": o["p"],
                         "ratio_days_plus1": o["ratio"],
                         "ratio_ci_low": o["ratio_low"],
                         "ratio_ci_high": o["ratio_high"],
                         "n": out["_meta"]["n"], "projects": out["_meta"]["projects"],
                         "r2": out["_meta"]["r2"]})
    reg = pd.DataFrame(reg_rows)
    reg.to_csv(RESULTS / "table3_regression.csv", index=False)

    # Effective sample: issues, projects, PRs, commits.
    rel_shas = pd.read_parquet(RESULTS / "relevant_shas.parquet")
    eff_rows = []
    for g in GROUP_ORDER:
        sub = df[df.group == g]
        prs = set()
        for proj, v in zip(sub.project, sub.pr_numbers_files):
            if pd.notna(v) and str(v).strip():
                for pr_num in str(v).split(";"):
                    if pr_num.strip():
                        prs.add((proj, pr_num.strip()))
        rs = rel_shas.merge(sub[["project", "issue_number"]],
                            on=["project", "issue_number"], how="inner")
        eff_rows.append({"group": g, "issues": len(sub),
                         "projects": sub.project.nunique(),
                         "unique_linked_prs": len(prs),
                         "unique_commits": rs.sha.nunique(),
                         "top_project": sub.project.value_counts().index[0] if len(sub) else "",
                         "top_project_share_pct": 100 * sub.project.value_counts().iloc[0] / len(sub) if len(sub) else np.nan})
    pd.DataFrame(eff_rows).to_csv(RESULTS / "table_effective_sample.csv", index=False)

    # Leave-one-project-out FE estimates for projects contributing to each group.
    loo_rows = []
    for g in ("asset_only", "mixed"):
        projects = df[df.group == g].project.value_counts()
        if g == "mixed":
            projects = projects.head(12)
        for proj in projects.index:
            d = df[df.project != proj]
            try:
                _, o = fit_log_model(d, fe=True)
                if g in o:
                    x = o[g]
                    loo_rows.append({"excluded_project": proj, "group": g,
                                     "ratio": x["ratio"], "ci_low": x["ratio_low"],
                                     "ci_high": x["ratio_high"], "p_value": x["p"],
                                     "n": len(d)})
            except Exception:
                continue
    pd.DataFrame(loo_rows).to_csv(RESULTS / "table6_leave_one_project_out.csv", index=False)

    resid = res.resid
    diag_lines += [f"Residual skew={stats.skew(resid):.3f}, kurtosis={stats.kurtosis(resid):.3f}",
                   f"Residual median={np.median(resid):.4f}, IQR={np.percentile(resid,75)-np.percentile(resid,25):.4f}",
                   "Inference: OLS on log1p(days) with project-clustered robust SE; "
                   "coefficients are multiplicative shifts in (days+1), not percent changes in days."]
    with open(RESULTS / "model_diagnostics.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(diag_lines) + "\n")

    # Within-project paired comparisons
    rows = []
    dat_all = []
    for other, label in [("asset_only", "asset_only vs code_only"),
                         ("mixed", "mixed vs code_only")]:
        r, dat = paired_project_stats(df, other, label)
        if r:
            rows.append(r)
            dat["comparison"] = label
            dat_all.append(dat)
    paired = pd.DataFrame(rows)
    paired.to_csv(RESULTS / "table4_project_paired.csv", index=False)
    if dat_all:
        pd.concat(dat_all).to_csv(RESULTS / "project_paired_data.csv", index=False)

    # Sensitivity analyses
    sens = []

    def add_sens(name, d, group_col="group", balanced=False):
        d = d[d[group_col].isin(GROUP_ORDER)].copy()
        d["group"] = pd.Categorical(d[group_col], categories=GROUP_ORDER)
        counts = d.group.value_counts().to_dict()
        if counts.get("code_only", 0) < 3 or counts.get("asset_only", 0) < 3:
            sens.append({"scenario": name, "n": len(d), "n_code": counts.get("code_only", 0),
                         "n_asset": counts.get("asset_only", 0),
                         "n_mixed": counts.get("mixed", 0), "note": "too few observations"})
            return
        if balanced:
            pm = d.groupby(["project", "group"], observed=True).duration_days.median().unstack()
            pm = pm.dropna(subset=["code_only", "asset_only"])
            md_asset = float((pm.asset_only - pm.code_only).median())
            md_mixed = float((pm["mixed"] - pm.code_only).median()) if "mixed" in pm else np.nan
            sens.append({"scenario": name, "n": len(d), "projects_used": len(pm),
                         "n_code": counts.get("code_only", 0),
                         "n_asset": counts.get("asset_only", 0),
                         "n_mixed": counts.get("mixed", 0),
                         "median_asset_minus_code": md_asset,
                         "median_mixed_minus_code": md_mixed,
                         "note": "project medians, equal project weight"})
            return
        b, dd = cluster_bootstrap(d, B=600, seed=RNG_SEED + len(sens))
        lo_a, hi_a = ci95(dd["asset_minus_code"])
        lo_m, hi_m = ci95(dd["mixed_minus_code"])
        xa = d[d[group_col] == "asset_only"].duration_days.to_numpy(float)
        xc = d[d[group_col] == "code_only"].duration_days.to_numpy(float)
        xm = d[d[group_col] == "mixed"].duration_days.to_numpy(float)
        p_a = stats.mannwhitneyu(xa, xc, alternative="two-sided").pvalue if len(xa) and len(xc) else np.nan
        try:
            _, mo = fit_log_model(d, fe=False)
            adj_a = mo.get("asset_only", {}).get("ratio", np.nan)
            adj_m = mo.get("mixed", {}).get("ratio", np.nan)
        except Exception:
            adj_a = adj_m = np.nan
        sens.append({"scenario": name, "n": len(d), "n_code": counts.get("code_only", 0),
                     "n_asset": counts.get("asset_only", 0),
                     "n_mixed": counts.get("mixed", 0),
                     "median_code": float(np.median(xc)) if len(xc) else np.nan,
                     "median_asset": float(np.median(xa)) if len(xa) else np.nan,
                     "median_asset_minus_code": float(np.median(xa) - np.median(xc)) if len(xa) and len(xc) else np.nan,
                     "ci_low": lo_a, "ci_high": hi_a,
                     "median_mixed_minus_code": float(np.median(xm) - np.median(xc)) if len(xm) and len(xc) else np.nan,
                     "mixed_ci_low": lo_m, "mixed_ci_high": hi_m,
                     "mwu_p_asset_vs_code": p_a,
                     "adj_ratio_asset_vs_code": adj_a,
                     "adj_ratio_mixed_vs_code": adj_m})

    add_sens("Primary: label or text, all merged PRs", df)
    top_asset = df[df.group == "asset_only"].project.value_counts().idxmax()
    add_sens(f"Exclude dominant asset project ({top_asset})", df[df.project != top_asset])
    add_sens("Bug label only", ds[ds.bug_label == True])
    add_sens("Text rule positive", ds[ds.bug_text == True])
    add_sens("Exclude closure > 365 days", df[df.duration_days <= 365])
    dw = df.copy()
    cap = dw.duration_days.quantile(.99)
    dw["duration_days"] = dw.duration_days.clip(upper=cap)
    add_sens(f"Winsorize at p99 ({cap:.1f} days)", dw)
    add_sens("Exclude non-fix labels (duplicate/invalid/wontfix/stale)", df[~df.nonfix_label.astype(bool)])
    add_sens("Single linked PR only", df[df.n_linked_prs == 1])
    add_sens("Issue closed on/after first merge", df[df.closed_after_merge == True])
    add_sens("Largest linked PR only", ds, group_col="group_largest")
    add_sens("Earliest merged PR only", ds, group_col="group_earliest")
    add_sens("Exclude <1-day closures", df[df.duration_days >= 1.0])
    add_sens("Exclude bot-authored issues", df[~df.contributor.astype(str).str.contains(r"\[bot\]|bot$", regex=True, case=False)])
    add_sens("Project-balanced (median per project)", df, balanced=True)
    sens = pd.DataFrame(sens)
    sens.to_csv(RESULTS / "table5_sensitivity.csv", index=False)

    print(f"[stats] {time.time()-t0:.1f}s n={len(df)} "
          f"KW p={p_kw:.3g} medians={df.groupby('group', observed=True).duration_days.median().to_dict()}")
    return df, desc, tests, reg, paired, sens


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------
def _style():
    plt.rcParams.update({
        "font.family": "DejaVu Serif", "font.size": 10,
        "axes.titlesize": 11, "axes.labelsize": 10,
        "axes.spines.top": False, "axes.spines.right": False,
        "figure.dpi": 150, "savefig.dpi": 300, "savefig.bbox": "tight",
    })


def savefig(fig, name):
    fig.savefig(FIGDIR / f"{name}.png")
    fig.savefig(FIGDIR / f"{name}.pdf")


def fig_flow():
    flow = pd.read_csv(RESULTS / "table_sample_flow.csv")
    val = dict(zip(flow.stage, flow.n))
    total = int(val["All issue records in OSSGameBench"])
    closed = int(val["Closed non-PR issues with valid timestamps"])
    bug = int(val["Bug reports by label or conservative text rule"])
    linked = int(val["Linked to >=1 merged PR"])
    files = int(val["At least one changed file recovered"])
    known = int(val["Final known-group analysis sample"])
    code = int(val["Classified as code-only"])
    asset = int(val["Classified as asset-only"])
    mixed = int(val["Classified as mixed"])

    fig, ax = plt.subplots(figsize=(7.6, 8.0))
    ax.set_xlim(0, 11.5); ax.set_ylim(0, 10); ax.axis("off")
    boxes = [
        (4.6, 9.3, f"OSSGameBench issue records\nn = {total:,}"),
        (4.6, 7.9, f"Closed, non-PR issues with valid timestamps\nn = {closed:,}"),
        (4.6, 6.5, f"Bug reports: issue label or conservative text rule\nn = {bug:,}"),
        (4.6, 5.1, f"Linked to at least one merged PR\nn = {linked:,}"),
        (4.6, 3.7, f"Issue-level changed-file set recovered\nn = {files:,}"),
        (4.6, 2.3, f"Known-group analysis sample\nn = {known:,}"),
    ]
    for x, y, txt in boxes:
        ax.add_patch(FancyBboxPatch((x - 3.0, y - 0.48), 6.0, 0.86,
                                    boxstyle="round,pad=0.03,rounding_size=0.08",
                                    fc="#EAF0F6", ec="#33475B", lw=1.1))
        ax.text(x, y, txt, ha="center", va="center", fontsize=8.0)
    for y in [8.62, 7.22, 5.82, 4.42, 3.02]:
        ax.add_patch(FancyArrowPatch((4.6, y), (4.6, y - 0.18), arrowstyle="-|>",
                                     mutation_scale=11, color="#33475B"))
    # exclusions
    side = [
        (8.0, 8.6, f"Open issues / missing closed_at: {total-closed:,}"),
        (8.0, 7.2, f"Not identified as bug: {closed-bug:,}"),
        (8.0, 5.8, f"No merged linked PR: {bug-linked:,}"),
        (8.0, 4.4, f"No commit file recovered: {linked-files:,}"),
        (8.0, 3.0, f"Docs/config only: {files-known:,}"),
    ]
    for x, y, txt in side:
        ax.annotate(txt, xy=(7.6, y), xytext=(x, y), fontsize=6.8,
                    ha="left", va="center", color="#555555",
                    arrowprops=dict(arrowstyle="-", color="#999999", lw=0.7))
    # group boxes
    for x, label, n, color in [(2.2, "Code-only", code, GROUP_COLOR["code_only"]),
                               (4.6, "Asset-only", asset, GROUP_COLOR["asset_only"]),
                               (7.0, "Mixed", mixed, GROUP_COLOR["mixed"])]:
        ax.add_patch(FancyBboxPatch((x - 1.08, 0.55), 2.16, 0.72,
                                    boxstyle="round,pad=0.03,rounding_size=0.07",
                                    fc=color, ec=color, alpha=0.88))
        ax.text(x, 0.91, f"{label}\nn = {n:,}", ha="center", va="center",
                color="white", fontsize=8.2, weight="bold")
    for x in (2.2, 4.6, 7.0):
        ax.add_patch(FancyArrowPatch((4.6, 1.75), (x, 1.3), arrowstyle="-|>",
                                     mutation_scale=10, color="#666666"))
    ax.text(5.7, 9.95, "Figure 1. Bug-report sample construction", ha="center",
            fontsize=11, weight="bold")
    savefig(fig, "figure1_sample_flow")
    plt.close(fig)


def fig_counts(df):
    counts = df.group.value_counts().reindex(GROUP_ORDER)
    fig, ax = plt.subplots(figsize=(6.0, 3.8))
    bars = ax.bar([GROUP_LABEL[g] for g in GROUP_ORDER], counts.values,
                  color=[GROUP_COLOR[g] for g in GROUP_ORDER], width=0.62,
                  edgecolor="black", linewidth=0.6)
    for b, v in zip(bars, counts.values):
        ax.text(b.get_x() + b.get_width() / 2, v + max(counts) * 0.015,
                f"{v:,}\n({100*v/counts.sum():.1f}%)", ha="center", va="bottom", fontsize=9)
    ax.set_ylabel("Number of bug reports")
    ax.set_title("Figure 2. Bug reports by modification type")
    ax.set_ylim(0, max(counts) * 1.22)
    ax.grid(axis="y", alpha=0.25, linewidth=0.6)
    savefig(fig, "figure2_group_counts")
    plt.close(fig)


def fig_box(df):
    fig, ax = plt.subplots(figsize=(6.6, 4.4))
    data = [np.log10(df[df.group == g].duration_days.to_numpy(float) + 1) for g in GROUP_ORDER]
    bp = ax.boxplot(data, patch_artist=True, widths=0.55, showfliers=False,
                    medianprops=dict(color="black", lw=1.4),
                    whiskerprops=dict(color="#444444"), capprops=dict(color="#444444"))
    for patch, g in zip(bp["boxes"], GROUP_ORDER):
        patch.set_facecolor(GROUP_COLOR[g]); patch.set_alpha(0.8)
        patch.set_edgecolor("black"); patch.set_linewidth(0.6)
    for i, g in enumerate(GROUP_ORDER):
        x = df[df.group == g].duration_days.to_numpy(float)
        ax.scatter(i + 1, np.log10(np.median(x) + 1), marker="D", s=28,
                   color="white", edgecolor="black", zorder=5, linewidth=0.8)
        ax.text(i + 1, ax.get_ylim()[1] if False else 0, "", ha="center")
    ax.set_xticks([1, 2, 3])
    ax.set_xticklabels([f"{GROUP_LABEL[g]}\n(n = {int((df.group==g).sum()):,})" for g in GROUP_ORDER])
    ticks = [0, 1, 7, 30, 90, 365, 730, 1825, 3650]
    ax.set_yticks([np.log10(v + 1) for v in ticks])
    ax.set_yticklabels([str(v) for v in ticks])
    ax.set_ylabel("Issue closure time (days; log scale)")
    ax.set_title("Closure time by modification type")
    ax.grid(axis="y", alpha=0.25, linewidth=0.6)
    savefig(fig, "figure3_closure_boxplot")
    plt.close(fig)


def _dumbbell(ax, data, other, title):
    data = data.dropna(subset=["code_only", other]).copy()
    data = data[(data.n_code >= 3) & (data.n_other >= 3)]
    if len(data) == 0:
        ax.text(0.5, 0.5, "Insufficient projects", ha="center", va="center",
                transform=ax.transAxes)
        ax.set_title(title)
        return
    data = data.sort_values("diff")
    y = np.arange(len(data))
    ax.hlines(y, data.code_only, data[other], color="#BBBBBB", lw=1.4, zorder=1)
    ax.scatter(data.code_only, y, color=GROUP_COLOR["code_only"], s=28,
               label="Code-only", zorder=3)
    ax.scatter(data[other], y, color=GROUP_COLOR[other], s=28,
               label=GROUP_LABEL[other], zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels([p.split("/")[-1][:24] for p in data.project], fontsize=6.4)
    ax.set_xscale("log")
    ax.set_xlabel("Median closure time (days; log scale)")
    ax.set_title(title)
    ax.grid(axis="x", alpha=0.25, linewidth=0.6)


def fig_projects():
    dat = pd.read_csv(RESULTS / "project_paired_data.csv")
    fig, axes = plt.subplots(1, 2, figsize=(9.6, 5.8))
    for ax, other, title in [(axes[0], "asset_only", "(a) Asset-only vs code-only (>=3 issues/group)"),
                             (axes[1], "mixed", "(b) Mixed vs code-only (>=3 issues/group)")]:
        sub = dat[dat.comparison == f"{other} vs code_only"]
        _dumbbell(ax, sub, other, title)
    from matplotlib.lines import Line2D
    h = [Line2D([0], [0], marker="o", ls="", color=GROUP_COLOR[g],
                label=GROUP_LABEL[g], ms=7) for g in GROUP_ORDER]
    l = [GROUP_LABEL[g] for g in GROUP_ORDER]
    fig.legend(h, l, loc="lower center", ncol=3, frameon=False, fontsize=8,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle("Figure 4. Within-project median closure times", fontsize=11, weight="bold")
    fig.tight_layout(rect=[0, 0.07, 1, 0.95])
    savefig(fig, "figure4_project_differences")
    plt.close(fig)


def fig_forest():
    reg = pd.read_csv(RESULTS / "table7_stepwise_models.csv")
    keep = reg.copy()
    keep["term_label"] = keep.term.map({"asset_only": "Asset-only", "mixed": "Mixed"})
    keep["row"] = keep.model_id + " " + keep.term_label
    keep = keep.iloc[::-1]
    fig, ax = plt.subplots(figsize=(7.2, 5.4))
    y = np.arange(len(keep))
    colors = [GROUP_COLOR["asset_only"] if t == "asset_only" else GROUP_COLOR["mixed"]
              for t in keep.term]
    ax.errorbar(keep.ratio_days_plus1, y,
                xerr=[keep.ratio_days_plus1 - keep.ci_low,
                      keep.ci_high - keep.ratio_days_plus1],
                fmt="o", color="none", ecolor="#555555", capsize=3, lw=1.1, ms=0)
    ax.scatter(keep.ratio_days_plus1, y, c=colors, s=42, zorder=3, edgecolor="black", linewidth=0.5)
    ax.axvline(1, color="black", lw=0.9, ls="--")
    ax.set_yticks(y); ax.set_yticklabels(keep.row, fontsize=7.6)
    ax.set_xscale("log")
    ax.set_xlim(0.55, 5.2)
    from matplotlib.ticker import FixedLocator, FixedFormatter, NullLocator
    ticks = [0.6, 1.0, 1.5, 2.0, 3.0, 4.0]
    ax.xaxis.set_major_locator(FixedLocator(ticks))
    ax.xaxis.set_major_formatter(FixedFormatter(["0.6", "1", "1.5", "2", "3", "4"]))
    ax.xaxis.set_minor_locator(NullLocator())
    ax.set_xlabel("Ratio of (closure days + 1) versus code-only")
    ax.set_title("Stepwise effect estimates with 95% CI")
    ax.grid(axis="x", alpha=0.25, linewidth=0.6)
    for yi, (_, r) in zip(y, keep.iterrows()):
        ax.text(r.ci_high * 1.05, yi,
                f"{r.ratio_days_plus1:.2f} [{r.ci_low:.2f}, {r.ci_high:.2f}]",
                va="center", fontsize=6.6)
    savefig(fig, "figure5_effect_estimates")
    plt.close(fig)


def stage_figures():
    _style()
    ds = pd.read_csv(RESULTS / "analysis_dataset.csv")
    df = ds[ds.group.isin(GROUP_ORDER)].copy()
    fig_flow()
    fig_counts(df)
    fig_box(df)
    fig_projects()
    fig_forest()
    print("[figures] written to", FIGDIR)


# ---------------------------------------------------------------------------
# Validation artefacts
# ---------------------------------------------------------------------------
def _short(x, n=400):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    s = re.sub(r"\s+", " ", str(x)).strip()
    return s[:n]


def stage_validation(force: bool = False):
    ensure_dirs()
    cls_path = VALIDATION / "classification_audit_sample.csv"
    bug_path = VALIDATION / "bug_identification_audit_sample.csv"
    if not force and cls_path.exists() and bug_path.exists():
        # Preserve existing completed/in-progress audit files unless --force is passed
        try:
            c_df = pd.read_csv(cls_path, dtype=str).fillna("")
            b_df = pd.read_csv(bug_path, dtype=str).fillna("")
            if (c_df.get("review_status", pd.Series()).str.lower().eq("reviewed").any() or
                    b_df.get("review_status", pd.Series()).str.lower().eq("reviewed").any()):
                print("[validation] existing reviewed audit samples detected; preserving (pass --force to regenerate)")
                return
        except Exception:
            pass
    ds = pd.read_csv(RESULTS / "analysis_dataset.csv")
    base = pd.read_parquet(RESULTS / "analysis_base.parquet")
    rng = np.random.default_rng(RNG_SEED)

    # Classification audit: stratified random sample.
    strata = [("code_only", 50), ("asset_only", 30), ("mixed", 50),
              ("other", 25), ("unknown", 25)]
    parts = []
    for g, n in strata:
        sub = ds[ds.group == g]
        if len(sub) == 0:
            continue
        take = sub.sample(min(n, len(sub)), random_state=int(rng.integers(1, 1_000_000)))
        parts.append(take)
    audit = pd.concat(parts, ignore_index=True)
    audit = audit.sample(frac=1.0, random_state=RNG_SEED).reset_index(drop=True)
    body_all = pd.read_parquet(DATA / "issue.parquet", columns=["project", "number", "body"]).rename(
        columns={"number": "issue_number"})
    audit = audit.merge(body_all, on=["project", "issue_number"], how="left")
    criteria = ("Classify only the changed files: code_only = CODE with no ASSET; "
                "asset_only = ASSET with no CODE; mixed = both; other = only OTHER; "
                "unknown = no file recovered. Ignore docs/config when deciding CODE vs ASSET.")
    out = pd.DataFrame({
        "audit_id": [f"C{i:03d}" for i in range(1, len(audit) + 1)],
        "stratum": audit.group,
        "project": audit.project,
        "issue_number": audit.issue_number,
        "issue_url": "https://github.com/" + audit.project + "/issues/" + audit.issue_number.astype(str),
        "issue_title": audit.title.map(lambda x: _short(x, 220)),
        "issue_body_preview": audit.body.map(lambda x: _short(x, 900)),
        "issue_labels": audit.labels,
        "linked_prs": audit.pr_numbers_files,
        "closure_days": audit.duration_days.round(3),
        "n_files": audit.n_files,
        "n_code": audit.n_code,
        "n_asset": audit.n_asset,
        "n_other": audit.n_other,
        "changed_files_preview": audit.file_examples.map(lambda x: _short(x, 900)),
        "classification_criteria": criteria,
        "auto_group": audit.group,
        "reviewer_group": "",
        "reviewer_confidence_1to5": "",
        "reviewer_notes": "",
        "review_status": "PENDING",
    })
    out.to_csv(VALIDATION / "classification_audit_sample.csv", index=False)

    # Bug identification audit: label/text strata.
    bug = ds.copy()
    bug["stratum"] = np.select(
        [bug.bug_label & bug.bug_text, bug.bug_label & ~bug.bug_text,
         ~bug.bug_label & bug.bug_text],
        ["label_and_text", "label_only", "text_only"], default="neither")
    bparts = []
    for s, n in [("label_and_text", 20), ("label_only", 20),
                 ("text_only", 20), ("neither", 20)]:
        sub = bug[bug.stratum == s]
        if len(sub) == 0:
            continue
        bparts.append(sub.sample(min(n, len(sub)), random_state=int(rng.integers(1, 1_000_000))))
    # Add true non-bug controls from the full closed-issue base.
    neg = base[~base.bug_any].sample(20, random_state=RNG_SEED).copy()
    neg["issue_number"] = neg.number
    neg["pr_numbers_files"] = neg.pr_numbers
    neg["stratum"] = "neither"
    bparts.append(neg)
    baud = pd.concat(bparts, ignore_index=True).sample(frac=1.0, random_state=RNG_SEED).reset_index(drop=True)
    base_body = pd.read_parquet(DATA / "issue.parquet", columns=["project", "number", "body"]).rename(
        columns={"number": "issue_number"})
    baud = baud.merge(base_body, on=["project", "issue_number"], how="left")
    bout = pd.DataFrame({
        "audit_id": [f"B{i:03d}" for i in range(1, len(baud) + 1)],
        "stratum": baud.stratum,
        "project": baud.project,
        "issue_number": baud.issue_number,
        "issue_url": "https://github.com/" + baud.project + "/issues/" + baud.issue_number.astype(str),
        "issue_title": baud.title.map(lambda x: _short(x, 220)),
        "issue_body_preview": baud.body.map(lambda x: _short(x, 900)),
        "issue_labels": baud.labels,
        "auto_bug_label": baud.bug_label,
        "auto_bug_text": baud.bug_text,
        "auto_bug_any": baud.bug_any,
        "decision_criteria": ("Yes = a defect, crash, regression, incorrect behavior, or broken feature; "
                              "No = feature request, question, documentation, duplicate/invalid administrative item, "
                              "or no defect claim; Unclear = insufficient information."),
        "linked_prs": baud.pr_numbers_files,
        "closure_days": baud.duration_days.round(3),
        "reviewer_is_bug": "",
        "reviewer_confidence_1to5": "",
        "reviewer_notes": "",
        "review_status": "PENDING",
    })
    bout.to_csv(VALIDATION / "bug_identification_audit_sample.csv", index=False)

    # Commit / linkage coverage.
    log = pd.read_csv(RESULTS / "commit_scan_log.csv")
    log["coverage_pct"] = 100 * log.matched_commits / log.wanted_commits.replace(0, np.nan)
    log.to_csv(VALIDATION / "commit_coverage.csv", index=False)
    cov = pd.DataFrame([{
        "wanted_commits": int(log.wanted_commits.sum()),
        "matched_commits": int(log.matched_commits.sum()),
        "commit_coverage_pct": 100 * log.matched_commits.sum() / log.wanted_commits.sum(),
        "projects_scanned": len(log),
        "projects_complete": int((log.matched_commits >= log.wanted_commits).sum()),
        "projects_zero_match": int((log.matched_commits == 0).sum()),
        "commit_file_rows": int(log.matched_file_rows.sum()),
    }])
    cov.to_csv(VALIDATION / "commit_coverage_summary.csv", index=False)

    # Sample flow by classification and project concentration.
    known = ds[ds.group.isin(GROUP_ORDER)]
    conc = (known.groupby(["group", "project"]).size().rename("issues").reset_index()
            .sort_values(["group", "issues"], ascending=[True, False]))
    conc.to_csv(VALIDATION / "project_concentration.csv", index=False)

    qa_path = VALIDATION / "data_quality_checks.json"
    qa = json.load(open(qa_path, encoding="utf-8")) if qa_path.exists() else {}
    qa.update({
        "commit_coverage_pct": round(float(cov.commit_coverage_pct.iloc[0]), 2),
        "projects_complete_commit_coverage": int(cov.projects_complete.iloc[0]),
        "projects_zero_commit_match": int(cov.projects_zero_match.iloc[0]),
        "asset_only_share_top_project_pct": round(
            100 * known[known.group == "asset_only"].project.value_counts().iloc[0] /
            (known.group == "asset_only").sum(), 2),
        "classification_audit_n": int(len(out)),
        "classification_audit_status": "PENDING_HUMAN_REVIEW",
        "bug_audit_n": int(len(bout)),
        "bug_audit_status": "PENDING_HUMAN_REVIEW",
    })
    json.dump(qa, open(qa_path, "w", encoding="utf-8"), indent=2)
    pd.DataFrame([{"check": k, "value": v} for k, v in qa.items()]).to_csv(
        VALIDATION / "data_quality_checks.csv", index=False)

    status = {
        "classification_audit": {
            "file": "classification_audit_sample.csv",
            "target": len(out), "reviewed": 0, "status": "PENDING_HUMAN_REVIEW",
        },
        "bug_identification_audit": {
            "file": "bug_identification_audit_sample.csv",
            "target": len(bout), "reviewed": 0, "status": "PENDING_HUMAN_REVIEW",
        },
        "note": "No human labels have been entered. Review columns are intentionally blank.",
    }
    json.dump(status, open(VALIDATION / "audit_status.json", "w", encoding="utf-8"), indent=2)

    rules = """# Bug identification rules

A closed, non-PR issue is treated as a bug report if either rule fires.

## R1: issue label (primary evidence)
Positive labels match (case-insensitive, whole token):
`bug`, `bugs`, `bugfix`, `bug fix`, `bug report`, `defect(s)`, `regression`,
`crash`, `crash or hang`, `error report`.

Negated or clearly non-defect labels veto R1:
`not a bug`, `no bug`, `non-bug`, `notabug`, `feature request`, `enhancement`,
`question`, `discussion`, `suggestion`, `idea`, `documentation`.

## R2: conservative text rule (fallback)
The title must contain a defect term (`bug`, `crash`, `error`, `exception`,
`broken`, `fail(s|ure)`, `glitch`, `freeze`, `hang`, `regression`, `incorrect`,
`wrong`, `does not work`, `not working`), **and** the body must contain a
reproduction/report marker (`steps to reproduce`, `to reproduce`, `expected
behavior`, `actual behavior`, `stack trace`, `traceback`, `system/version
information`). A title beginning with `[Bug]`, `Bug:`, `[Crash]`, etc. is
sufficient.

Text-only positives are 10,348 closed issues (12.2% of the 84,954 bug sample).
The label-only sensitivity analysis removes them.

## Exclusions
- Open issues and issues with a missing `closed_at` are excluded.
- Records whose number matches a pull request in the same project are excluded.
- Negative-duration records are excluded (none found).
- Issues carrying `duplicate`, `invalid`, `wontfix`, `stale`, `not a bug`,
  `cannot reproduce`, `question`, `feature request`, or `enhancement` are kept
  in the primary sample and removed in a sensitivity analysis.

## Human review
`bug_identification_audit_sample.csv` contains 80 stratified cases
(label+text, label only, text only, neither). Review columns are blank; no
human judgments are fabricated in this package.
"""
    (VALIDATION / "bug_identification_rules.md").write_text(rules, encoding="utf-8")

    taxonomy = """# File classification taxonomy

Each changed path is assigned to CODE, ASSET or OTHER.

- **CODE**: source and scripting languages, HTML/CSS, shaders, build files
  (`CMakeLists.txt`, `Makefile`, Gradle), and engine scripts.
- **ASSET**: raster/vector images, audio, video, 3-D/2-D models, animation,
  fonts, tile maps, atlases, Unity/Godot scene, prefab and material assets,
  and authored map files (`.vmf`, `.bsp`, `.oramap`).
- **OTHER**: documentation, localization, configuration, schemas, metadata,
  package manifests, binary libraries, and unknown extensions.

Context overrides prevent common false positives: images under `docs/`,
`documentation/`, `fastlane/`, `dist/` or test/fixture directories are OTHER;
Unity `ProjectSettings/` and settings assets are OTHER; dependency/metadata
basenames such as `go.mod`, `go.sum`, `copyright`, and `license` are OTHER
(preventing `go.mod` from matching tracker audio `.mod`). Package archives
(`.zip`, `.pak`, `.wad`) are not counted as art assets.

Group rule (issue level, union of all merged linked PR file sets):
- `code_only`: >=1 CODE and 0 ASSET
- `asset_only`: >=1 ASSET and 0 CODE
- `mixed`: both
- `other`: neither (only OTHER paths)
- `unknown`: no file recovered

OTHER files do not by themselves change the CODE/ASSET contrast (for example,
a code fix that also edits a changelog remains code-only). This rule is stated
in the report and audited in `classification_audit_sample.csv`.
"""
    (VALIDATION / "classification_taxonomy.md").write_text(taxonomy, encoding="utf-8")

    protocol = """# Audit protocol

No human labels have been entered. `review_status` is `PENDING` for every row,
and `audit_status.json` records zero completed reviews.

## Classification audit

`classification_audit_sample.csv` contains 180 issues: 50 code-only, 30
asset-only, 50 mixed, 25 other and 25 unknown, sampled with seed 20261119.

Each row provides the original issue title and body preview, issue URL, labels,
linked PRs, closure time, file counts, changed-file preview, the automatic group
and the classification rule. Reviewers must judge the **changed files**, not
the issue title or the automatic group.

Allowed `reviewer_group` values:

- `code_only`: at least one CODE file and no ASSET file.
- `asset_only`: at least one ASSET file and no CODE file.
- `mixed`: both CODE and ASSET files.
- `other`: files recovered, but only OTHER (docs/config/localization/etc.).
- `unknown`: no changed file recovered.

OTHER files do not change the CODE/ASSET contrast. If the file preview is
insufficient, open `issue_url` and the linked PR. Enter a confidence score
1-5, notes, and `review_status=reviewed`. Two independent reviewers should
compute Cohen's kappa after completing the sample.

## Bug identification audit

`bug_identification_audit_sample.csv` contains 80 issues in four strata:
label+text, label only, text only, and non-bug controls. Each row provides the
original title and body preview, URL, labels, automatic flags, linked PRs and
the decision rule.

Allowed `reviewer_is_bug` values:

- `Yes`: a defect, crash, regression, incorrect behaviour, or broken feature.
- `No`: feature request, question, documentation, administrative duplicate or
  invalid item, or no defect claim.
- `Unclear`: insufficient information.

Enter a confidence score 1-5, notes, and `review_status=reviewed`.

## Status

`audit_status.json` records zero completed reviews. The analysis in this package
does not depend on uncollected human labels. `audit_evaluation.py` uses
inverse-stratum weights; unweighted accuracy is diagnostic only and must not be
reported as the population estimate.
"""
    (VALIDATION / "audit_protocol.md").write_text(protocol, encoding="utf-8")
    print(f"[validation] classification={len(out)} bug={len(bout)}")


# ---------------------------------------------------------------------------
# Final robustness extension: stepwise models, project influence, coverage
# ---------------------------------------------------------------------------
def _fit_spec(df: pd.DataFrame, rhs: str, outcome: str = "log1p"):
    d = df.copy()
    d["size_log"] = np.log1p(d.n_files.clip(lower=1))
    if outcome == "log1p":
        d["_y"] = np.log1p(d.duration_days)
    elif outcome == "log":
        d = d[d.duration_days > 0].copy()
        d["_y"] = np.log(d.duration_days)
    else:
        raise ValueError(outcome)
    res = smf.ols("_y ~ " + rhs, data=d).fit(
        cov_type="cluster", cov_kwds={"groups": d.project})
    out = {}
    for term in res.params.index:
        key = "asset_only" if "asset_only" in term else ("mixed" if "mixed" in term else None)
        if key:
            ci = res.conf_int().loc[term]
            out[key] = {
                "beta": float(res.params[term]), "ratio": float(math.exp(res.params[term])),
                "ci_low": float(math.exp(ci[0])), "ci_high": float(math.exp(ci[1])),
                "p": float(res.pvalues[term]),
            }
    meta = {"n": int(res.nobs), "projects": int(d.project.nunique()),
            "r2": float(res.rsquared), "r2_adj": float(res.rsquared_adj)}
    return out, meta


def stage_extended():
    ensure_dirs()
    d = pd.read_csv(RESULTS / "analysis_sample.csv")
    d["group"] = pd.Categorical(d.group, categories=GROUP_ORDER)
    g = "C(group, Treatment('code_only'))"

    # 1. Stepwise models with a common outcome and clustered SE.
    specs = [
        ("S1", "Type only", g),
        ("S2", "Type + creation year", g + " + C(created_year)"),
        ("S3", "Type + year + project FE", g + " + C(created_year) + C(project)"),
        ("S4", "Type + year + size", g + " + C(created_year) + size_log"),
        ("S5", "Full: type + year + size + project FE",
         g + " + C(created_year) + size_log + C(project)"),
    ]
    rows = []
    s1 = {}
    for mid, label, rhs in specs:
        out, meta = _fit_spec(d, rhs)
        for term, o in out.items():
            if mid == "S1":
                s1[term] = o["ratio"]
            rows.append({
                "model_id": mid, "model": label, "term": term, "beta": o["beta"],
                "ratio_days_plus1": o["ratio"], "ci_low": o["ci_low"],
                "ci_high": o["ci_high"], "p_value": o["p"],
                "pct_change_vs_S1": 100 * (o["ratio"] / s1.get(term, o["ratio"]) - 1),
                "n": meta["n"], "projects": meta["projects"],
                "r2": meta["r2"], "r2_adj": meta["r2_adj"],
            })
    step = pd.DataFrame(rows)
    step.to_csv(RESULTS / "table7_stepwise_models.csv", index=False)

    # 2. Transform check: log1p(days) versus log(days).
    tr_rows = []
    for outcome, label, interp in [
        ("log1p", "log1p", "ratio of (closure days + 1)"),
        ("log", "log", "ratio of geometric mean closure days"),
    ]:
        out, meta = _fit_spec(d, specs[-1][2], outcome=outcome)
        for term, o in out.items():
            tr_rows.append({"outcome": label, "term": term, "ratio": o["ratio"],
                            "ci_low": o["ci_low"], "ci_high": o["ci_high"],
                            "p_value": o["p"], "n": meta["n"],
                            "projects": meta["projects"], "interpretation": interp})
    pd.DataFrame(tr_rows).to_csv(RESULTS / "table8_transform_sensitivity.csv", index=False)

    # 3. Asset-only project sensitivity and leave-one-project-out.
    full_rhs = specs[-1][2]
    asset_projects = d[d.group == "asset_only"].project.value_counts().index.tolist()
    sens_rows = []

    def add_asset_row(label, sub, excluded="", bootstrap=False):
        out, meta = _fit_spec(sub, full_rhs)
        a = sub[sub.group == "asset_only"].duration_days
        c = sub[sub.group == "code_only"].duration_days
        row = {"scenario": label, "excluded_project": excluded,
               "n_asset": len(a), "n_code": len(c),
               "median_asset": float(a.median()) if len(a) else np.nan,
               "median_code": float(c.median()) if len(c) else np.nan,
               "raw_diff_days": float(a.median() - c.median()) if len(a) and len(c) else np.nan,
               "full_ratio_days_plus1": out.get("asset_only", {}).get("ratio", np.nan),
               "ci_low": out.get("asset_only", {}).get("ci_low", np.nan),
               "ci_high": out.get("asset_only", {}).get("ci_high", np.nan),
               "p_value": out.get("asset_only", {}).get("p", np.nan),
               "n": meta["n"], "projects": meta["projects"]}
        row["ci_excludes_1"] = bool(row["ci_low"] > 1 or row["ci_high"] < 1) if pd.notna(row["ci_low"]) else False
        row["raw_ci_low"] = np.nan
        row["raw_ci_high"] = np.nan
        if bootstrap:
            _, dd = cluster_bootstrap(sub[sub.group.isin(["code_only", "asset_only"])],
                                      B=1000, seed=RNG_SEED + len(sens_rows))
            lo, hi = ci95(dd["asset_minus_code"])
            row["raw_ci_low"], row["raw_ci_high"] = lo, hi
        sens_rows.append(row)

    add_asset_row("All asset-only projects", d, bootstrap=True)
    add_asset_row("Exclude reactivedrop", d[d.project != "reactivedrop/reactivedrop_public_src"],
                  excluded="reactivedrop/reactivedrop_public_src", bootstrap=True)
    for proj in asset_projects:
        add_asset_row("Leave-one-project-out", d[d.project != proj], excluded=proj)
    asset_sens = pd.DataFrame(sens_rows)
    asset_sens.to_csv(RESULTS / "table9_asset_project_sensitivity.csv", index=False)

    loo = asset_sens[asset_sens.scenario == "Leave-one-project-out"]
    summary = pd.DataFrame([{
        "all_ratio": asset_sens.iloc[0].full_ratio_days_plus1,
        "exclude_reactivedrop_ratio": asset_sens.iloc[1].full_ratio_days_plus1,
        "loo_min_ratio": loo.full_ratio_days_plus1.min(),
        "loo_median_ratio": loo.full_ratio_days_plus1.median(),
        "loo_max_ratio": loo.full_ratio_days_plus1.max(),
        "loo_ci_excluding_1": int(loo.ci_excludes_1.sum()),
        "loo_projects": int(len(loo)),
        "loo_raw_diff_min": loo.raw_diff_days.min(),
        "loo_raw_diff_max": loo.raw_diff_days.max(),
    }])
    summary.to_csv(RESULTS / "table9b_asset_sensitivity_summary.csv", index=False)

    # 4. Paired-sample reconciliation for the 29/30 mixed result.
    paired = pd.read_csv(RESULTS / "project_paired_data.csv")
    pp = paired[paired.comparison == "mixed vs code_only"].project.tolist()
    pd_ = d[d.project.isin(pp) & d.group.isin(["code_only", "mixed"])].copy()
    prs = []
    for mid, label, rhs in specs:
        out, meta = _fit_spec(pd_, rhs)
        o = out.get("mixed", {})
        prs.append({"model_id": mid, "model": label,
                    "ratio_days_plus1": o.get("ratio", np.nan),
                    "ci_low": o.get("ci_low", np.nan), "ci_high": o.get("ci_high", np.nan),
                    "p_value": o.get("p", np.nan), "n": meta["n"],
                    "projects": meta["projects"], "r2": meta["r2"],
                    "median_files_code": float(pd_[pd_.group == "code_only"].n_files.median()),
                    "median_files_mixed": float(pd_[pd_.group == "mixed"].n_files.median())})
    pd.DataFrame(prs).to_csv(RESULTS / "table11_paired_reconciliation.csv", index=False)

    # 5. Coverage funnel with explicit denominators.
    links = pd.read_parquet(RESULTS / "bug_issue_links.parquet")
    rel = pd.read_parquet(RESULTS / "relevant_shas.parquet")
    cf = pd.read_parquet(RESULTS / "commit_files_raw.parquet")
    base = pd.read_parquet(RESULTS / "analysis_base.parquet")
    bug = links[links.bug_any == True]
    found = rel.merge(cf[["project", "sha"]].drop_duplicates(), on=["project", "sha"])
    qa = json.load(open(VALIDATION / "data_quality_checks.json", encoding="utf-8"))
    log = pd.read_csv(RESULTS / "commit_scan_log.csv")

    bug_issues = int(base.bug_any.sum())
    any_linked = bug[["project", "issue_number"]].drop_duplicates().shape[0]
    merged_issues = bug[bug.pr_merged][["project", "issue_number"]].drop_duplicates().shape[0]
    commit_issues = bug[bug.pr_merged & bug.pr_has_commits][["project", "issue_number"]].drop_duplicates().shape[0]
    file_issues = found[["project", "issue_number"]].drop_duplicates().shape[0]
    known_issues = int((d.group.isin(GROUP_ORDER)).sum())

    linked_prs = bug[["project", "pr_number"]].drop_duplicates().shape[0]
    merged_prs = bug[bug.pr_merged][["project", "pr_number"]].drop_duplicates().shape[0]
    commit_prs = bug[bug.pr_merged & bug.pr_has_commits][["project", "pr_number"]].drop_duplicates().shape[0]
    file_prs = found[["project", "pr_number"]].drop_duplicates().shape[0]

    wanted_records = int(log.wanted_commits.sum())
    matched_records = int(log.matched_commits.sum())
    wanted_unique = int(rel.sha.nunique())
    matched_unique = int(found.sha.nunique())
    projects_scanned = int(len(log))
    projects_complete = int((log.matched_commits >= log.wanted_commits).sum())
    projects_zero = int((log.matched_commits == 0).sum())

    def pct(a, b):
        return round(100 * a / b, 2) if b else np.nan

    total_issues_dedup = int(qa.get("raw_issues", 319709))
    closed_issues_n = int(len(base))
    raw_links_n = int(qa.get("raw_issue_pr_links", len(links)))
    uniq_links_n = int(qa.get("unique_issue_pr_links", len(links)))
    valid_links_n = int(qa.get("valid_issue_pr_links", len(links)))
    miss_iss_n = int(qa.get("links_to_missing_issue", 0))
    miss_pr_n = int(qa.get("links_to_missing_pr", 0))
    dedup_proj_n = int(qa.get("dedup_projects", 950))
    # Restrict commit_scan_log to deduplicated projects
    log_dedup = log[log.project.isin(set(rel.project.unique()))] if len(rel) else log
    wanted_records = int(log_dedup.wanted_commits.sum())
    matched_records = int(log_dedup.matched_commits.sum())
    projects_scanned = int(len(log_dedup))
    projects_complete = int((log_dedup.matched_commits >= log_dedup.wanted_commits).sum())
    projects_zero = int((log_dedup.matched_commits == 0).sum())

    cov_rows = [
        {"level": "issue", "stage": "Closed non-PR issue universe", "numerator": closed_issues_n,
         "denominator": total_issues_dedup, "percent": pct(closed_issues_n, total_issues_dedup),
         "definition": "Closed issues with valid created_at/closed_at, excluding PR records"},
        {"level": "issue", "stage": "Bug reports", "numerator": bug_issues,
         "denominator": closed_issues_n, "percent": pct(bug_issues, closed_issues_n),
         "definition": "Closed bug reports by label or conservative text rule"},
        {"level": "issue", "stage": "Bug issues with any linked PR", "numerator": any_linked,
         "denominator": bug_issues, "percent": pct(any_linked, bug_issues),
         "definition": "At least one valid issue-PR link, merged or not"},
        {"level": "issue", "stage": "Bug issues with a merged linked PR", "numerator": merged_issues,
         "denominator": bug_issues, "percent": pct(merged_issues, bug_issues),
         "definition": "At least one linked PR with state MERGED"},
        {"level": "issue", "stage": "Bug issues with a merged PR and commits", "numerator": commit_issues,
         "denominator": bug_issues, "percent": pct(commit_issues, bug_issues),
         "definition": "Merged PR has >=1 row in pr_commit"},
        {"level": "issue", "stage": "Bug issues with recovered changed files", "numerator": file_issues,
         "denominator": bug_issues, "percent": pct(file_issues, bug_issues),
         "definition": "At least one linked commit found in per-project commit parquet"},
        {"level": "issue", "stage": "Bug issues in known-group sample", "numerator": known_issues,
         "denominator": bug_issues, "percent": pct(known_issues, bug_issues),
         "definition": "Changed files classify as code_only, asset_only or mixed"},
        {"level": "link", "stage": "Raw issue-PR link rows", "numerator": raw_links_n,
         "denominator": raw_links_n, "percent": 100.0,
         "definition": "Link rows in issue_pr.parquet before deduplication"},
        {"level": "link", "stage": "Unique issue-PR pairs", "numerator": uniq_links_n,
         "denominator": raw_links_n, "percent": pct(uniq_links_n, raw_links_n),
         "definition": "Exact duplicate issue-PR pairs removed"},
        {"level": "link", "stage": "Links to issues in cleaned table", "numerator": valid_links_n,
         "denominator": uniq_links_n, "percent": pct(valid_links_n, uniq_links_n),
         "definition": "Issue number exists in the closed non-PR issue table"},
        {"level": "link", "stage": "Links to missing issue", "numerator": miss_iss_n,
         "denominator": uniq_links_n, "percent": pct(miss_iss_n, uniq_links_n),
         "definition": "Issue number absent from the cleaned issue table"},
        {"level": "link", "stage": "Links to missing PR", "numerator": miss_pr_n,
         "denominator": uniq_links_n, "percent": pct(miss_pr_n, uniq_links_n),
         "definition": "PR number absent from pr.parquet (among unique issue-PR pairs)"},
        {"level": "PR", "stage": "Unique bug-linked PRs", "numerator": linked_prs,
         "denominator": linked_prs, "percent": 100.0,
         "definition": "Distinct (project, pr_number) among valid bug issue-PR links"},
        {"level": "PR", "stage": "Merged", "numerator": merged_prs,
         "denominator": linked_prs, "percent": pct(merged_prs, linked_prs),
         "definition": "PR state MERGED"},
        {"level": "PR", "stage": "Merged with commits", "numerator": commit_prs,
         "denominator": linked_prs, "percent": pct(commit_prs, linked_prs),
         "definition": "Merged PR with >=1 row in pr_commit"},
        {"level": "PR", "stage": "With recovered changed files", "numerator": file_prs,
         "denominator": linked_prs, "percent": pct(file_prs, linked_prs),
         "definition": "At least one PR commit found in commit parquet"},
        {"level": "commit", "stage": "PR-commit records recovered", "numerator": matched_records,
         "denominator": wanted_records, "percent": pct(matched_records, wanted_records),
         "definition": "Matched records among wanted PR commits"},
        {"level": "commit", "stage": "Unique PR commits recovered", "numerator": matched_unique,
         "denominator": wanted_unique, "percent": pct(matched_unique, wanted_unique),
         "definition": "Distinct SHAs matched among distinct wanted SHAs"},
        {"level": "project", "stage": "Projects with wanted commits", "numerator": projects_scanned,
         "denominator": dedup_proj_n, "percent": pct(projects_scanned, dedup_proj_n),
         "definition": "Projects in deduplicated OSSGameBench with >=1 wanted bug-linked commit"},
        {"level": "project", "stage": "Projects with complete commit recovery", "numerator": projects_complete,
         "denominator": projects_scanned, "percent": pct(projects_complete, projects_scanned),
         "definition": "All wanted commits for the project were found"},
        {"level": "project", "stage": "Projects with zero commit recovery", "numerator": projects_zero,
         "denominator": projects_scanned, "percent": pct(projects_zero, projects_scanned),
         "definition": "No wanted commit for the project was found"},
    ]
    pd.DataFrame(cov_rows).to_csv(RESULTS / "table10_coverage_funnel.csv", index=False)

    ds_all = pd.read_csv(RESULTS / "analysis_dataset.csv")
    other_only_n = int((ds_all.group == "other").sum())
    no_merged_n = int((ds_all.n_merged_prs == 0).sum())
    no_files_n = int(((ds_all.n_merged_prs > 0) & (ds_all.n_files == 0)).sum())
    non_bug_n = int(len(base) - len(ds_all))
    open_n = int(total_issues_dedup - len(base))
    loss = [
        ("All issue records", total_issues_dedup, "Starting frame (950 deduplicated projects)"),
        ("Open or missing closed_at", open_n, "Right-censored / no closure timestamp"),
        ("Not identified as a bug", non_bug_n, "Neither label nor conservative text rule"),
        ("No merged linked PR", no_merged_n, "No explicit merged issue-PR link"),
        ("No recovered commit file", no_files_n, "Merged PR but no reachable commit file"),
        ("Only other/unclassifiable files", other_only_n, "No CODE and no ASSET file"),
        ("Known-group analysis sample", known_issues, "Code-only, asset-only or mixed"),
    ]
    assert sum(x[1] for x in loss[1:]) == loss[0][1]
    pd.DataFrame(loss, columns=["stage", "n", "reason"]).to_csv(
        RESULTS / "table12_sample_loss.csv", index=False)

    # 6. Per-issue commit recovery completeness sensitivity check.
    cf_shas = cf[["project", "sha"]].drop_duplicates().assign(_rec=True)
    m_rec = rel.drop_duplicates(["project", "issue_number", "sha"]).merge(
        cf_shas, on=["project", "sha"], how="left")
    m_rec["_rec"] = m_rec["_rec"].fillna(False)
    per_iss = m_rec.groupby(["project", "issue_number"]).agg(
        expected_shas=("sha", "size"), recovered_shas=("_rec", "sum")).reset_index()
    per_iss["complete"] = per_iss.recovered_shas == per_iss.expected_shas
    dc = d.merge(per_iss, on=["project", "issue_number"], how="left")
    dc_full = dc[dc.complete == True].copy()
    dc_full["group"] = pd.Categorical(dc_full.group, categories=GROUP_ORDER)
    out_c, meta_c = _fit_spec(dc_full, specs[-1][2], outcome="log1p")
    out_cl, _ = _fit_spec(dc_full, specs[-1][2], outcome="log")
    comp_rows = [{
        "n_total": len(dc),
        "n_complete": len(dc_full),
        "complete_pct": round(100 * len(dc_full) / len(dc), 2),
        "code_complete_pct": round(100 * dc[dc.group == "code_only"].complete.mean(), 2),
        "asset_complete_pct": round(100 * dc[dc.group == "asset_only"].complete.mean(), 2),
        "mixed_complete_pct": round(100 * dc[dc.group == "mixed"].complete.mean(), 2),
        "n_code_complete": int((dc_full.group == "code_only").sum()),
        "n_asset_complete": int((dc_full.group == "asset_only").sum()),
        "n_mixed_complete": int((dc_full.group == "mixed").sum()),
        "median_code_complete": float(dc_full[dc_full.group == "code_only"].duration_days.median()),
        "median_asset_complete": float(dc_full[dc_full.group == "asset_only"].duration_days.median()),
        "median_mixed_complete": float(dc_full[dc_full.group == "mixed"].duration_days.median()),
        "s5_asset_ratio": out_c["asset_only"]["ratio"],
        "s5_asset_ci_low": out_c["asset_only"]["ci_low"],
        "s5_asset_ci_high": out_c["asset_only"]["ci_high"],
        "s5_asset_p": out_c["asset_only"]["p"],
        "s5_mixed_ratio": out_c["mixed"]["ratio"],
        "s5_mixed_ci_low": out_c["mixed"]["ci_low"],
        "s5_mixed_ci_high": out_c["mixed"]["ci_high"],
        "s5_mixed_p": out_c["mixed"]["p"],
        "s5_log_asset_ratio": out_cl["asset_only"]["ratio"],
        "s5_log_asset_p": out_cl["asset_only"]["p"],
        "s5_log_mixed_ratio": out_cl["mixed"]["ratio"],
        "s5_log_mixed_p": out_cl["mixed"]["p"],
    }]
    pd.DataFrame(comp_rows).to_csv(RESULTS / "table21_complete_recovery_check.csv", index=False)

    qa.update({
        "issue_coverage_any_pr_pct": pct(any_linked, bug_issues),
        "issue_coverage_merged_pr_pct": pct(merged_issues, bug_issues),
        "issue_coverage_files_pct": pct(file_issues, bug_issues),
        "issue_coverage_known_group_pct": pct(known_issues, bug_issues),
        "pr_coverage_files_pct": pct(file_prs, linked_prs),
        "commit_record_recovery_pct": pct(matched_records, wanted_records),
        "commit_unique_recovery_pct": pct(matched_unique, wanted_unique),
        "coverage_denominator_note": "31.6% is commit-record recovery, not issue coverage",
    })
    json.dump(qa, open(VALIDATION / "data_quality_checks.json", "w", encoding="utf-8"), indent=2)
    pd.DataFrame([{"check": k, "value": v} for k, v in qa.items()]).to_csv(
        VALIDATION / "data_quality_checks.csv", index=False)

    coverage_md = f"""# Coverage definitions

The {pct(matched_records, wanted_records):.1f}% figure is **commit-record recovery**, not issue coverage.

```
commit recovery = matched wanted PR-commit records / wanted PR-commit records
                = {matched_records:,} / {wanted_records:,} = {pct(matched_records, wanted_records):.1f}%
unique commit recovery = {matched_unique:,} / {wanted_unique:,} = {pct(matched_unique, wanted_unique):.1f}%
```

Issue coverage uses the {bug_issues:,} closed bug reports as its denominator:

```
any linked PR           {any_linked:,} / {bug_issues:,} = {pct(any_linked, bug_issues):.1f}%
merged linked PR        {merged_issues:,} / {bug_issues:,} = {pct(merged_issues, bug_issues):.1f}%
merged PR + commits     {commit_issues:,} / {bug_issues:,} = {pct(commit_issues, bug_issues):.1f}%
recovered changed files  {file_issues:,} / {bug_issues:,} =  {pct(file_issues, bug_issues):.1f}%
known-group sample       {known_issues:,} / {bug_issues:,} =  {pct(known_issues, bug_issues):.1f}%
```

PR coverage uses distinct bug-linked PRs:

```
linked PRs              {linked_prs:,}
merged                  {merged_prs:,} / {linked_prs:,} = {pct(merged_prs, linked_prs):.1f}%
merged + commits        {commit_prs:,} / {linked_prs:,} = {pct(commit_prs, linked_prs):.1f}%
recovered changed files  {file_prs:,} / {linked_prs:,} = {pct(file_prs, linked_prs):.1f}%
```

Link-level attrition:

```
raw issue-PR rows       78,060
unique issue-PR pairs   69,099 / 78,060 = 88.5%
links to missing issue   6,163 / 69,099 =  8.9%
links to missing PR        137 / 69,099 =  0.2%
```

The denominators differ because one issue can link to several PRs, one PR can
close several issues, and one commit can serve several issues. A commit can be
missing even when the issue-PR link is valid: PR head commits are not always
reachable from the cloned default branch (squash merges, deleted branches,
fork-only commits). Missing commits are classified as `unknown`; they are not
imputed.

Project coverage: {projects_scanned} of {dedup_proj_n} deduplicated projects have at least one wanted commit; {projects_complete}
have complete recovery and {projects_zero} have none. The known-group sample covers {int(d.project.nunique())}
projects. These are project-level counts, not issue-level coverage.
"""
    (VALIDATION / "coverage_definitions.md").write_text(coverage_md, encoding="utf-8")

    print("[extended] wrote stepwise, transform, asset-sensitivity, paired and coverage tables")
    return step, asset_sens, pd.DataFrame(cov_rows)



# ---------------------------------------------------------------------------
# Stage 8: Selection bias and modification-size common support (Tables 13-20, Fig 6)
# ---------------------------------------------------------------------------
def cliff(x, y):
    x = np.asarray(x, float)
    y = np.asarray(y, float)
    if len(x) == 0 or len(y) == 0:
        return np.nan
    u = stats.mannwhitneyu(x, y, alternative="two-sided").statistic
    return 2 * u / (len(x) * len(y)) - 1


def bootstrap_median_diff(df, group_col, a, b, B=1000, seed=RNG_SEED):
    d = df[df[group_col].isin([a, b])].copy()
    y = d.duration_days.to_numpy(float)
    g = d[group_col].to_numpy()
    codes, _ = pd.factorize(d.project.to_numpy())
    idx_by = [np.flatnonzero(codes == i) for i in range(codes.max() + 1)]
    rng = np.random.default_rng(seed)
    vals = []
    for _ in range(B):
        pick = rng.integers(0, len(idx_by), len(idx_by))
        idx = np.concatenate([idx_by[i] for i in pick])
        gg, yy = g[idx], y[idx]
        va, vb = yy[gg == a], yy[gg == b]
        if len(va) and len(vb):
            vals.append(np.median(va) - np.median(vb))
    vals = np.asarray(vals)
    return (np.nanpercentile(vals, 2.5), np.nanpercentile(vals, 97.5))


def bug_cohorts():
    iss = pd.read_parquet(ROOT / "data" / "issue.parquet",
                          columns=["project", "number", "contributor", "title", "body",
                                   "labels", "state", "created_at", "closed_at"])
    dup_projects = find_duplicate_projects(iss)
    if dup_projects:
        iss = iss[~iss.project.isin(dup_projects)].copy()
    iss["created_at"] = parse_dt(iss.created_at)
    iss["closed_at"] = parse_dt(iss.closed_at)
    iss["labels_list"] = iss.labels.map(parse_labels)
    flags = iss.labels_list.map(bug_label_flags)
    iss["bug_label"] = [x[0] for x in flags]
    iss["nonfix"] = [x[1] for x in flags]
    iss["bug_text"] = [bug_text_flag(t, b) for t, b in zip(iss.title, iss.body)]
    iss["bug_any"] = iss.bug_label | iss.bug_text
    bug = iss[iss.bug_any].copy()
    bug["duration_days"] = (bug.closed_at - bug.created_at).dt.total_seconds() / 86400

    sample = pd.read_csv(RESULTS / "analysis_sample.csv")
    keys = set(zip(sample.project, sample.issue_number))
    bug["included"] = [(p, n) in keys for p, n in zip(bug.project, bug.number)]
    ad = pd.read_csv(RESULTS / "analysis_dataset.csv",
                     usecols=["project", "issue_number", "n_linked_prs", "n_merged_prs", "n_files", "group"])
    bug = bug.merge(ad, left_on=["project", "number"],
                    right_on=["project", "issue_number"], how="left")

    def reason(r):
        if r.state != "CLOSED" or pd.isna(r.closed_at):
            return "open_or_missing_closed"
        if pd.isna(r.n_merged_prs) or r.n_merged_prs == 0:
            return "no_merged_linked_pr"
        if pd.isna(r.n_files) or r.n_files == 0:
            return "merged_pr_no_recovered_files"
        if r.group == "other":
            return "only_other_files"
        return "other_excluded"

    bug["exclusion_reason"] = bug.apply(reason, axis=1)
    bug.loc[bug.included, "exclusion_reason"] = "included_known_group"
    bug["cohort"] = np.select(
        [bug.included,
         (~bug.included) & (bug.state == "CLOSED"),
         (~bug.included) & (bug.state != "CLOSED")],
        ["included", "excluded_closed", "excluded_open"], default="other")
    return bug


def selection_tables(bug):
    counts = bug.groupby(["cohort", "exclusion_reason"]).size().rename("n").reset_index()
    counts["pct_of_all_bug"] = 100 * counts.n / len(bug)
    counts.to_csv(RESULTS / "table13_selection_cohorts.csv", index=False)

    inc = bug[bug.cohort == "included"]
    exc = bug[bug.cohort == "excluded_closed"]
    opn = bug[bug.cohort == "excluded_open"]
    bug["title_len"] = bug.title.astype(str).str.len()
    bug["body_len"] = bug.body.astype(str).str.len()
    bug["body_empty"] = bug.body.astype(str).str.strip().eq("")
    bug["is_bot"] = bug.contributor.astype(str).str.contains(r"\[bot\]|bot$", case=False, regex=True)
    bug["created_year"] = bug.created_at.dt.year

    def p_cont(col):
        a, b = inc[col].dropna(), exc[col].dropna()
        return stats.mannwhitneyu(a, b, alternative="two-sided").pvalue if len(a) and len(b) else np.nan

    def p_bin(col):
        closed_bug = bug[bug.cohort.isin(["included", "excluded_closed"])]
        tab = pd.crosstab(closed_bug.cohort.eq("included"), closed_bug[col].fillna(False))
        if tab.shape[1] < 2:
            return np.nan
        return stats.chi2_contingency(tab)[1]

    for df in (inc, exc, opn):
        df["title_len"] = df.title.astype(str).str.len()
        df["body_len"] = df.body.astype(str).str.len()
        df["body_empty"] = df.body.astype(str).str.strip().eq("")
        df["is_bot"] = df.contributor.astype(str).str.contains(r"\[bot\]|bot$", case=False, regex=True)
        df["created_year"] = df.created_at.dt.year

    feats = []
    def add(feature, a, b, c, p, test):
        feats.append({"feature": feature, "included": a, "excluded_closed": b,
                      "excluded_open": c, "p_included_vs_excluded_closed": p, "test": test})
    add("n", len(inc), len(exc), len(opn), np.nan, "")
    add("open_share_pct", 0.0, 0.0, 100.0, np.nan, "")
    add("median_closure_days", inc.duration_days.median(), exc.duration_days.median(), np.nan,
        p_cont("duration_days"), "Mann-Whitney")
    add("mean_closure_days", inc.duration_days.mean(), exc.duration_days.mean(), np.nan,
        np.nan, "")
    add("median_created_year", inc.created_at.dt.year.median(), exc.created_at.dt.year.median(),
        opn.created_at.dt.year.median(), p_cont("created_year"), "Mann-Whitney")
    add("median_title_chars", inc.title_len.median(), exc.title_len.median(), opn.title_len.median(),
        p_cont("title_len"), "Mann-Whitney")
    add("median_body_chars", inc.body_len.median(), exc.body_len.median(), opn.body_len.median(),
        p_cont("body_len"), "Mann-Whitney")
    add("empty_body_pct", 100 * inc.body_empty.mean(), 100 * exc.body_empty.mean(),
        100 * opn.body_empty.mean(), p_bin("body_empty"), "chi-square")
    add("median_label_count", inc.labels_list.map(len).median(), exc.labels_list.map(len).median(),
        opn.labels_list.map(len).median(), np.nan, "")
    add("nonfix_label_pct", 100 * inc.nonfix.mean(), 100 * exc.nonfix.mean(), 100 * opn.nonfix.mean(),
        p_bin("nonfix"), "chi-square")
    add("bot_author_pct", 100 * inc.is_bot.mean(), 100 * exc.is_bot.mean(), 100 * opn.is_bot.mean(),
        p_bin("is_bot"), "chi-square")
    add("median_linked_prs", inc.n_linked_prs.median(), exc.n_linked_prs.median(),
        opn.n_linked_prs.median(), np.nan, "")
    add("projects", inc.project.nunique(), exc.project.nunique(), opn.project.nunique(), np.nan, "")
    pd.DataFrame(feats).to_csv(RESULTS / "table14_selection_features.csv", index=False)

    # Project-level inclusion.
    proj = bug.groupby("project").agg(
        bug_issues=("number", "size"),
        included=("included", "sum"),
        open_issues=("state", lambda s: int((s != "CLOSED").sum())),
        median_closure_all=("duration_days", "median"),
    ).reset_index()
    proj["inclusion_rate_pct"] = 100 * proj.included / proj.bug_issues
    proj = proj.sort_values(["included", "bug_issues"], ascending=False)
    proj.to_csv(RESULTS / "table15_selection_projects.csv", index=False)

    # Closure-time comparison among closed bugs only.
    a = inc.duration_days.dropna().to_numpy(float)
    b = exc.duration_days.dropna().to_numpy(float)
    U, p = stats.mannwhitneyu(a, b, alternative="two-sided")
    lo, hi = bootstrap_median_diff(bug[bug.cohort.isin(["included", "excluded_closed"])],
                                   "cohort", "included", "excluded_closed", B=1000)
    q1a, q3a = np.percentile(a, [25, 75])
    q1b, q3b = np.percentile(b, [25, 75])
    pd.DataFrame([{
        "included_n": len(a), "excluded_n": len(b),
        "included_median": np.median(a), "excluded_median": np.median(b),
        "median_diff": np.median(a) - np.median(b),
        "median_diff_ci_low": lo, "median_diff_ci_high": hi,
        "included_q1": q1a, "included_q3": q3a, "excluded_q1": q1b, "excluded_q3": q3b,
        "included_mean": a.mean(), "excluded_mean": b.mean(),
        "cliffs_delta": 2 * U / (len(a) * len(b)) - 1, "mwu_p": p,
    }]).to_csv(RESULTS / "table16_selection_closure.csv", index=False)

    # Summary document.
    top10 = proj.head(10).included.sum() / len(inc) * 100
    md = f"""# Sample selection bias

## Denominator

- All bug reports (open + closed): **{len(bug):,}**
- Closed bug reports: **{len(inc) + len(exc):,}**
- Included known-group sample: **{len(inc):,}**
- The reported 8.5% is **{len(inc):,} / {len(inc) + len(exc):,}** (closed bug reports).
- As a share of all bug reports (including open), the sample is **{100*len(inc)/len(bug):.2f}%**.

## Why issues are excluded

| Reason | n |
|---|---:|
| Open or missing closure timestamp | {len(opn):,} |
| No merged linked PR | {int((bug.exclusion_reason=='no_merged_linked_pr').sum()):,} |
| Merged PR but no recovered commit file | {int((bug.exclusion_reason=='merged_pr_no_recovered_files').sum()):,} |
| Only other/unclassifiable files | {int((bug.exclusion_reason=='only_other_files').sum()):,} |
| Included known group | {len(inc):,} |

`merged_pr_no_recovered_files` means the issue has a merged linked PR but its
commit/file data could not be recovered. It is **not** evidence that the fix
changed no files. `no_merged_linked_pr` means no explicit merged issue-PR link
was available, not that no fix exists.

## Included versus excluded closed bugs

| Feature | Included | Excluded closed | p |
|---|---:|---:|---:|
| n | {len(inc):,} | {len(exc):,} | |
| Median closure (days) | {inc.duration_days.median():.2f} | {exc.duration_days.median():.2f} | {p:.2g} |
| Mean closure (days) | {inc.duration_days.mean():.2f} | {exc.duration_days.mean():.2f} | |
| Median creation year | {inc.created_at.dt.year.median():.0f} | {exc.created_at.dt.year.median():.0f} | |
| Non-fix label (%) | {100*inc.nonfix.mean():.2f} | {100*exc.nonfix.mean():.2f} | |
| Bot author (%) | {100*inc.is_bot.mean():.2f} | {100*exc.is_bot.mean():.2f} | |
| Projects | {inc.project.nunique()} | {exc.project.nunique()} | |

The included sample is slightly older on average, less likely to carry non-fix labels,
much less likely to be bot-authored, and has a small but statistically
detectable shorter median closure time (Cliff's delta
{2*U/(len(a)*len(b))-1:.3f}). The top ten projects contribute {top10:.1f}% of
the included sample. These differences are selection, not causal effects.

## Project distribution

{proj.head(12).to_string(index=False)}
"""
    (VALIDATION / "selection_bias.md").write_text(md, encoding="utf-8")
    return bug, counts, proj


def size_tables(bug):
    d = pd.read_csv(RESULTS / "analysis_sample.csv")
    groups = ["code_only", "asset_only", "mixed"]
    qs = [0, 5, 10, 25, 50, 75, 90, 95, 99, 100]
    rows = []
    for g in groups:
        x = d[d.group == g].n_files.to_numpy(float)
        r = {"group": g, "n": len(x), "mean": x.mean(), "sd": x.std(ddof=1)}
        for q in qs:
            r[f"p{q}"] = np.percentile(x, q)
        rows.append(r)
    qdf = pd.DataFrame(rows)
    qdf.to_csv(RESULTS / "table17_size_distribution.csv", index=False)

    code = d[d.group == "code_only"].n_files
    asset = d[d.group == "asset_only"].n_files
    mixed = d[d.group == "mixed"].n_files
    lo = max(np.percentile(code, 5), np.percentile(mixed, 5))
    hi = min(np.percentile(code, 95), np.percentile(mixed, 95))
    in_code = ((code >= lo) & (code <= hi)).sum()
    in_asset = ((asset >= lo) & (asset <= hi)).sum()
    in_mixed = ((mixed >= lo) & (mixed <= hi)).sum()
    cs = pd.DataFrame([{
        "common_support_low": lo, "common_support_high": hi,
        "code_n": len(code), "code_in_support": in_code,
        "code_in_support_pct": 100 * in_code / len(code),
        "asset_n": len(asset), "asset_in_support": in_asset,
        "asset_in_support_pct": 100 * in_asset / len(asset),
        "mixed_n": len(mixed), "mixed_in_support": in_mixed,
        "mixed_in_support_pct": 100 * in_mixed / len(mixed),
        "median_code": code.median(), "median_asset": asset.median(),
        "median_mixed": mixed.median(),
        "mixed_gt_50_pct": 100 * (mixed > 50).mean(),
        "code_gt_50_pct": 100 * (code > 50).mean(),
        "mixed_le_10_pct": 100 * (mixed <= 10).mean(),
        "code_le_10_pct": 100 * (code <= 10).mean(),
    }])
    cs.to_csv(RESULTS / "table18_size_common_support.csv", index=False)

    bins = [0, 1, 2, 3, 5, 10, 20, 50, 100, 10**9]
    labels = ["1", "2", "3", "4-5", "6-10", "11-20", "21-50", "51-100", ">100"]
    d["size_bin"] = pd.cut(d.n_files, bins=bins, labels=labels, right=True)
    rows = []
    for lab in labels:
        s = d[d.size_bin == lab]
        c = s[s.group == "code_only"].duration_days
        m = s[s.group == "mixed"].duration_days
        a = s[s.group == "asset_only"].duration_days
        p = np.nan
        diff = np.nan
        if len(c) >= 20 and len(m) >= 5:
            p = stats.mannwhitneyu(m, c, alternative="two-sided").pvalue
            diff = m.median() - c.median()
        rows.append({"size_bin": lab, "n_code": len(c), "n_asset": len(a), "n_mixed": len(m),
                     "median_code": c.median() if len(c) else np.nan,
                     "median_asset": a.median() if len(a) else np.nan,
                     "median_mixed": m.median() if len(m) else np.nan,
                     "mixed_minus_code": diff, "mwu_p": p})
    pd.DataFrame(rows).to_csv(RESULTS / "table19_size_stratified.csv", index=False)

    specs = [
        ("Full sample", 0, 10**9),
        ("n_files 1-10", 1, 10),
        ("n_files 2-10", 2, 10),
        ("5th-95th overlap", lo, hi),
        ("n_files 2-20", 2, 20),
        ("n_files 4-50", 4, 50),
    ]
    rhs = ("C(group, Treatment('code_only')) + C(created_year) + size_log + C(project)")
    rows = []
    for label, low, high in specs:
        s = d[(d.n_files >= low) & (d.n_files <= high)].copy()
        s["group"] = pd.Categorical(s.group, categories=groups)
        if len(s) == 0:
            continue
        try:
            out, meta = _fit_spec(s, rhs)
        except Exception as e:
            print("model failed", label, e)
            continue
        row = {"support": label, "low": low, "high": high,
               "n": len(s), "n_code": int((s.group == "code_only").sum()),
               "n_asset": int((s.group == "asset_only").sum()),
               "n_mixed": int((s.group == "mixed").sum()),
               "projects": int(s.project.nunique())}
        for term in ("asset_only", "mixed"):
            o = out.get(term, {})
            row[f"{term}_ratio"] = o.get("ratio", np.nan)
            row[f"{term}_ci_low"] = o.get("ci_low", np.nan)
            row[f"{term}_ci_high"] = o.get("ci_high", np.nan)
            row[f"{term}_p"] = o.get("p", np.nan)
        rows.append(row)
    pd.DataFrame(rows).to_csv(RESULTS / "table20_size_restricted_models.csv", index=False)

    # Figure 6.
    plt.rcParams.update({"font.family": "DejaVu Serif", "font.size": 10,
                         "axes.spines.top": False, "axes.spines.right": False})
    fig, axes = plt.subplots(1, 2, figsize=(10.0, 4.4))
    ax = axes[0]
    for g in groups:
        x = np.sort(d[d.group == g].n_files.to_numpy(float))
        y = np.arange(1, len(x) + 1) / len(x)
        ax.step(x, y, where="post", label=GROUP_LABEL[g], color=GROUP_COLOR[g], lw=1.6)
    ax.axvspan(lo, hi, color="#DDDDDD", alpha=0.45, zorder=0)
    ax.set_xscale("log")
    ax.set_xlabel("Changed files per issue (log scale)")
    ax.set_ylabel("Cumulative share of issues")
    ax.set_title("(a) ECDF of modification size")
    ax.grid(alpha=0.25, linewidth=0.6)
    ax.text(lo * 1.15, 0.25, f"common support\n[{lo:.0f}, {hi:.0f}]", fontsize=7.5, va="center",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.75))

    ax = axes[1]
    data = [np.log10(d[d.group == g].n_files.to_numpy(float)) for g in groups]
    bp = ax.boxplot(data, patch_artist=True, widths=0.55, showfliers=False,
                    medianprops=dict(color="black", lw=1.4))
    for patch, g in zip(bp["boxes"], groups):
        patch.set_facecolor(GROUP_COLOR[g]); patch.set_alpha(0.8)
    ax.set_xticks([1, 2, 3])
    ax.set_xticklabels([f"{GROUP_LABEL[g]}\n(n={int((d.group==g).sum()):,})" for g in groups])
    ticks = [1, 2, 3, 5, 10, 30, 100, 300, 1000]
    ax.set_yticks([np.log10(v) for v in ticks])
    ax.set_yticklabels([str(v) for v in ticks])
    ax.set_ylabel("Changed files per issue (log scale)")
    ax.set_title("(b) Distribution by modification type")
    ax.grid(axis="y", alpha=0.25, linewidth=0.6)
    from matplotlib.lines import Line2D
    handles = [Line2D([0], [0], color=GROUP_COLOR[g], lw=2, label=GROUP_LABEL[g]) for g in groups]
    fig.legend(handles=handles, loc="lower center", ncol=3, frameon=False, fontsize=8,
               bbox_to_anchor=(0.5, 0.0))
    fig.suptitle("Modification size and common support", fontsize=11, weight="bold")
    fig.tight_layout(rect=[0, 0.08, 1, 0.94])
    fig.savefig(FIGDIR / "figure6_modification_size.png")
    fig.savefig(FIGDIR / "figure6_modification_size.pdf")
    plt.close(fig)

    md = f"""# Modification-size common support

- Common support is defined as the overlap of the central 90% ranges:
  `[max(p5_code, p5_mixed), min(p95_code, p95_mixed)]` = **[{lo:.0f}, {hi:.0f}] files**.
- Median changed files: code-only **{code.median():.0f}**, asset-only
  **{asset.median():.0f}**, mixed **{mixed.median():.0f}**.
- Only **{in_mixed}/{len(mixed)} ({100*in_mixed/len(mixed):.1f}%)** mixed issues
  fall inside the common support; **{100*(mixed>50).mean():.1f}%** of mixed issues
  change more than 50 files, versus **{100*(code>50).mean():.1f}%** of code-only.
- Mixed issues cannot have one file: the minimum is {mixed.min():.0f}; code-only
  includes {int((code==1).sum()):,} one-file issues. The groups are structurally
  different in size.

Restricted full-model estimates are reported in `table20_size_restricted_models.csv`.
They are unstable across support definitions (mixed ratio ranges from below 1
to above 1), so they do not establish a size-independent effect. `n_files` may
be a confounder, a complexity proxy, or a mediator of mixed changes; the
adjusted change is not a causal mechanism.
"""
    (VALIDATION / "common_support.md").write_text(md, encoding="utf-8")
    return qdf, cs



def stage_final_checks():
    ensure_dirs()
    bug = bug_cohorts()
    selection_tables(bug)
    size_tables(bug)
    print("[final_checks] wrote tables 13-20 and figure6")


# ---------------------------------------------------------------------------
# Stage 9: Audit evaluation (inverse-stratum-weighted & exact Clopper-Pearson CIs)
# ---------------------------------------------------------------------------
audit_rng = np.random.default_rng(RNG_SEED)


def clopper_pearson_ci(k: int, n: int, alpha: float = 0.05) -> tuple[float, float]:
    lo = float(stats.beta.ppf(alpha / 2, k, n - k + 1)) if k > 0 else 0.0
    hi = float(stats.beta.ppf(1 - alpha / 2, k + 1, n - k)) if k < n else 1.0
    return lo, hi


def safe_div(a, b):
    return float(a) / float(b) if b else np.nan


def load_audit(path, kind):
    d = pd.read_csv(path, dtype=str).fillna("")
    human = "reviewer_group" if kind == "classification" else "reviewer_is_bug"
    d["_human"] = d[human].str.strip()
    d["_status"] = d["review_status"].str.strip().str.lower()
    d["_reviewed"] = (d["_status"] == "reviewed") & d["_human"].ne("")
    return d, human


def population_classification():
    d = pd.read_csv(RESULTS / "analysis_dataset.csv")
    return d.group.value_counts().to_dict()


def population_bug():
    d = pd.read_parquet(RESULTS / "analysis_base.parquet", columns=["bug_label", "bug_text", "bug_any"])
    d["stratum"] = np.select(
        [d.bug_label & d.bug_text, d.bug_label & ~d.bug_text, ~d.bug_label & d.bug_text],
        ["label_and_text", "label_only", "text_only"], default="neither")
    return d.stratum.value_counts().to_dict()


def weights_for(d, pop, stratum_col):
    n_s = d.groupby(stratum_col).size()
    return d[stratum_col].map(lambda s: pop.get(s, 0) / n_s.get(s, 1)).astype(float).to_numpy()


def weighted_confusion(auto, human, w, rows, cols):
    cm = pd.DataFrame(0.0, index=rows, columns=cols)
    for a, h, wi in zip(auto, human, w):
        if a in rows and h in cols:
            cm.loc[a, h] += wi
    return cm


def metrics_from_cm(cm, classes):
    out = {}
    for c in classes:
        tp = cm.loc[c, c] if c in cm.index and c in cm.columns else 0.0
        pred = cm.loc[c, :].sum() if c in cm.index else 0.0
        actual = cm.loc[:, c].sum() if c in cm.columns else 0.0
        out[c] = {"class": c, "support_weighted": actual,
                  "precision": safe_div(tp, pred), "recall": safe_div(tp, actual),
                  "tp_weighted": tp}
    return out


def bootstrap_cm(auto, human, w, strata, rows, cols, B=2000):
    out = []
    idx_by = [np.flatnonzero(strata == s) for s in pd.unique(strata)]
    for _ in range(B):
        chosen = []
        for idx in idx_by:
            if len(idx):
                chosen.append(audit_rng.choice(idx, size=len(idx), replace=True))
        idx = np.concatenate(chosen)
        cm = weighted_confusion(auto[idx], human[idx], w[idx], rows, cols)
        out.append(metrics_from_cm(cm, rows))
    return out


def evaluate_classification():
    d, human = load_audit(VALIDATION / "classification_audit_sample.csv", "classification")
    target = len(d)
    reviewed = int(d._reviewed.sum())
    if reviewed < target:
        return {"status": "PENDING", "reviewed": reviewed, "target": target,
                "message": "Not all classification rows have a confirmed human label."}
    pop = population_classification()
    d["stratum"] = d["auto_group"]
    w = weights_for(d, pop, "stratum")
    rows = ["code_only", "asset_only", "mixed", "other", "unknown"]
    cm = weighted_confusion(d.auto_group.to_numpy(), d._human.to_numpy(), w, rows, rows)
    cm.to_csv(VALIDATION / "audit_confusion_classification.csv")
    base = metrics_from_cm(cm, rows)
    boots = bootstrap_cm(d.auto_group.to_numpy(), d._human.to_numpy(), w,
                         d.stratum.to_numpy(), rows, rows, B=2000)
    out = []
    for r in base.values():
        c = r["class"]
        ps = [x[c]["precision"] for x in boots]
        rs = [x[c]["recall"] for x in boots]
        out.append({**r,
                    "precision_ci_low": np.nanpercentile(ps, 2.5) if np.any(~np.isnan(ps)) else np.nan,
                    "precision_ci_high": np.nanpercentile(ps, 97.5) if np.any(~np.isnan(ps)) else np.nan,
                    "recall_ci_low": np.nanpercentile(rs, 2.5) if np.any(~np.isnan(rs)) else np.nan,
                    "recall_ci_high": np.nanpercentile(rs, 97.5) if np.any(~np.isnan(rs)) else np.nan})
    pd.DataFrame(out).to_csv(VALIDATION / "audit_metrics_classification.csv", index=False)
    acc_w = np.trace(cm.to_numpy()) / cm.to_numpy().sum()
    acc_u = (d.auto_group.to_numpy() == d._human.to_numpy()).mean()
    return {"status": "COMPLETE", "reviewed": reviewed, "target": target,
            "weighted_accuracy": float(acc_w), "unweighted_accuracy": float(acc_u),
            "sample_weights": {k: pop.get(k, 0) / max(1, (d.stratum == k).sum()) for k in pop}}


def evaluate_bug():
    d, human = load_audit(VALIDATION / "bug_identification_audit_sample.csv", "bug")
    target = len(d)
    reviewed = int(d._reviewed.sum())
    if reviewed < target:
        return {"status": "PENDING", "reviewed": reviewed, "target": target,
                "message": "Not all bug-identification rows have a confirmed human label."}
    pop = population_bug()
    d["stratum"] = d["stratum"]
    w = weights_for(d, pop, "stratum")
    d["_auto"] = d.auto_bug_any.map({"True": "bug", "False": "non_bug"}).fillna("non_bug")
    d["_human2"] = d._human.map({"Yes": "bug", "No": "non_bug", "Unclear": "unclear"}).fillna("unclear")
    rows = ["bug", "non_bug"]
    cols = ["bug", "non_bug", "unclear"]
    cm = weighted_confusion(d._auto.to_numpy(), d._human2.to_numpy(), w, rows, cols)
    cm.to_csv(VALIDATION / "audit_confusion_bug.csv")
    base = metrics_from_cm(cm, rows)
    boots = bootstrap_cm(d._auto.to_numpy(), d._human2.to_numpy(), w,
                         d.stratum.to_numpy(), rows, cols, B=2000)
    out = []
    for r in base.values():
        c = r["class"]
        ps = [x[c]["precision"] for x in boots]
        rs = [x[c]["recall"] for x in boots]
        out.append({**r,
                    "precision_ci_low": np.nanpercentile(ps, 2.5) if np.any(~np.isnan(ps)) else np.nan,
                    "precision_ci_high": np.nanpercentile(ps, 97.5) if np.any(~np.isnan(ps)) else np.nan,
                    "recall_ci_low": np.nanpercentile(rs, 2.5) if np.any(~np.isnan(rs)) else np.nan,
                    "recall_ci_high": np.nanpercentile(rs, 97.5) if np.any(~np.isnan(rs)) else np.nan})
    pd.DataFrame(out).to_csv(VALIDATION / "audit_metrics_bug.csv", index=False)
    acc_w = np.trace(cm.to_numpy()[:, :2]) / cm.to_numpy()[:, :2].sum()
    acc_u = (d._auto.to_numpy() == d._human2.to_numpy()).mean()
    return {"status": "COMPLETE", "reviewed": reviewed, "target": target,
            "weighted_accuracy": float(acc_w), "unweighted_accuracy": float(acc_u),
            "sample_weights": {k: pop.get(k, 0) / max(1, (d.stratum == k).sum()) for k in pop}}


def write_pending(results):
    lines = ["# Audit evaluation", ""]
    for name, r in results.items():
        lines += [f"## {name}", "", f"Status: **{r['status']}**", "",
                  f"Reviewed: {r['reviewed']} / {r['target']}", "",
                  "No accuracy, precision, recall or confidence interval is computed while the audit is incomplete.",
                  ""]
    (VALIDATION / "audit_evaluation.md").write_text("\n".join(lines), encoding="utf-8")


def write_complete(results):
    lines = ["# Audit evaluation", "",
             "Estimates use inverse-stratum weights. Unweighted values are diagnostic only.", ""]
    for name, r in results.items():
        lines += [f"## {name}", "", f"Status: **{r['status']}**", "",
                  f"Reviewed: {r['reviewed']} / {r['target']}", "",
                  f"Weighted accuracy: {r['weighted_accuracy']:.3f}", "",
                  f"Unweighted accuracy (diagnostic): {r['unweighted_accuracy']:.3f}", "",
                  "Confusion matrix and per-class precision/recall with 95% bootstrap CIs are in the corresponding CSV files.",
                  ""]
    (VALIDATION / "audit_evaluation.md").write_text("\n".join(lines), encoding="utf-8")



def stage_audit_eval(audit_kind: str = "all"):
    ensure_dirs()
    results = {}
    if audit_kind in ("classification", "all"):
        results["classification"] = evaluate_classification()
    if audit_kind in ("bug", "all"):
        results["bug_identification"] = evaluate_bug()
    status = "COMPLETE" if all(r["status"] == "COMPLETE" for r in results.values()) else "PENDING"
    if status == "PENDING":
        for name in ["audit_confusion_classification.csv", "audit_metrics_classification.csv",
                     "audit_confusion_bug.csv", "audit_metrics_bug.csv"]:
            p = VALIDATION / name
            if p.exists():
                p.unlink()
    # Preserve existing reviewer_type metadata if present in audit_status.json
    st_path = VALIDATION / "audit_status.json"
    reviewer_type = "unknown"
    if st_path.exists():
        try:
            st_data = json.loads(st_path.read_text(encoding="utf-8"))
            reviewer_type = st_data.get("classification_audit", {}).get("reviewer_type", "unknown")
        except Exception:
            pass
    # Compute exact Clopper-Pearson per-stratum precision CIs for classification
    if status == "COMPLETE" and (VALIDATION / "classification_audit_sample.csv").exists():
        cd = pd.read_csv(VALIDATION / "classification_audit_sample.csv", dtype=str).fillna("")
        cp_rows = []
        for grp in ["code_only", "asset_only", "mixed", "other", "unknown"]:
            sub = cd[cd.auto_group == grp]
            n_g = len(sub)
            k_g = int((sub.reviewer_group == grp).sum())
            lo, hi = clopper_pearson_ci(k_g, n_g)
            cp_rows.append({"stratum": grp, "correct": k_g, "n": n_g,
                             "precision": k_g / n_g if n_g else np.nan,
                             "clopper_pearson_ci_low": lo, "clopper_pearson_ci_high": hi})
        results["classification"]["exact_stratum_precision_ci"] = cp_rows
    payload = {"status": status, "reviewer_type": reviewer_type, "audits": results,
               "sampling": "Stratified audit samples; inverse-stratum weights + exact Clopper-Pearson stratum CIs."}
    (VALIDATION / "audit_evaluation.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    if status == "PENDING":
        write_pending(results)
    else:
        write_complete(results)
    print(f"[audit_eval] status={status} reviewer_type={reviewer_type}")
    return payload


# ---------------------------------------------------------------------------
# Stage 10: Interactive audit review CLI
# ---------------------------------------------------------------------------
AUDIT_FILES = {
    "classification": VALIDATION / "classification_audit_sample.csv",
    "bug": VALIDATION / "bug_identification_audit_sample.csv",
}
ALLOWED = {
    "classification": ["code_only", "asset_only", "mixed", "other", "unknown"],
    "bug": ["Yes", "No", "Unclear"],
}


def _ar_load(path: Path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = reader.fieldnames or []
    for r in rows:
        r.setdefault("review_status", "PENDING")
    return fields, rows


def _ar_save(path: Path, fields: list[str], rows: list[dict]):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})
    os.replace(tmp, path)


def _ar__ar_wrap(text: str, width: int = 100, indent: str = "  ") -> str:
    text = "" if text is None else str(text)
    words = text.replace("\r", " ").replace("\n", " ").split()
    lines, cur = [], ""
    for word in words:
        if len(cur) + len(word) + 1 > width:
            lines.append(cur)
            cur = word
        else:
            cur = (cur + " " + word).strip()
    if cur:
        lines.append(cur)
    return "\n".join(indent + line for line in lines) if lines else indent + "(empty)"


def _ar_show(kind: str, row: dict, idx: int, total: int):
    print("\n" + "=" * 100)
    print(f"{row.get('audit_id','?')}  [{idx+1}/{total}]  status={row.get('review_status','PENDING')}")
    print("-" * 100)
    print(f"Project: {row.get('project','')}   Issue: #{row.get('issue_number','')}")
    print(f"URL: {row.get('issue_url','')}")
    print(f"Title: {row.get('issue_title','')}")
    print(f"Labels: {row.get('issue_labels','')}")
    print(f"Linked PRs: {row.get('linked_prs','')}")
    print(f"Closure days: {row.get('closure_days','')}")
    if kind == "classification":
        print(f"Auto group: {row.get('auto_group','')}")
        print(f"File counts: total={row.get('n_files','')} code={row.get('n_code','')} "
              f"asset={row.get('n_asset','')} other={row.get('n_other','')}")
        print("Changed files:")
        print(_ar_wrap(row.get("changed_files_preview", ""), 96))
        print("Rule:")
        print(_ar_wrap(row.get("classification_criteria", ""), 96))
        print(f"Human group: {row.get('reviewer_group','') or '(not set)'}   "
              f"confidence: {row.get('reviewer_confidence_1to5','') or '(not set)'}")
        print(f"Notes: {row.get('reviewer_notes','') or '(none)'}")
    else:
        print(f"Auto: label={row.get('auto_bug_label','')} text={row.get('auto_bug_text','')} "
              f"any={row.get('auto_bug_any','')}  stratum={row.get('stratum','')}")
        print("Rule:")
        print(_ar_wrap(row.get("decision_criteria", ""), 96))
        print(f"Human judgment: {row.get('reviewer_is_bug','') or '(not set)'}   "
              f"confidence: {row.get('reviewer_confidence_1to5','') or '(not set)'}")
        print(f"Notes: {row.get('reviewer_notes','') or '(none)'}")
    print("-" * 100)
    print("Issue text:")
    print(_ar_wrap(row.get("issue_body_preview", ""), 96))


def _ar__ar_ask_label(kind: str, current: str = "") -> str | None:
    allowed = ALLOWED[kind]
    prompt = f"Human label {allowed} (Enter=skip, q=quit): "
    while True:
        try:
            val = input(prompt).strip()
        except EOFError:
            return None
        if val.lower() == "q":
            return None
        if val == "":
            return ""
        if val in allowed:
            return val
        low = {x.lower(): x for x in allowed}
        if val.lower() in low:
            return low[val.lower()]
        print("Invalid label.")


def _ar__ar_ask_confidence(current: str = "") -> str | None:
    while True:
        try:
            val = input("Confidence 1-5 (Enter=skip, q=quit): ").strip()
        except EOFError:
            return None
        if val.lower() == "q":
            return None
        if val == "":
            return ""
        if val in {"1", "2", "3", "4", "5"}:
            return val
        print("Enter 1-5.")


def _ar_review_one(kind: str, row: dict) -> bool:
    label = _ar_ask_label(kind, row.get("reviewer_group" if kind == "classification" else "reviewer_is_bug", ""))
    if label is None:
        return False
    if label == "":
        return True
    conf = _ar_ask_confidence(row.get("reviewer_confidence_1to5", ""))
    if conf is None:
        return False
    if conf == "":
        return True
    note = input("Notes (optional): ").strip()
    if kind == "classification":
        row["reviewer_group"] = label
    else:
        row["reviewer_is_bug"] = label
    row["reviewer_confidence_1to5"] = conf
    row["reviewer_notes"] = note
    row["review_status"] = "reviewed"
    return True



def stage_audit_review(audit_kind: str, start_id: str | None = None):
    path = AUDIT_FILES[audit_kind]
    fields, rows = _ar_load(path)
    total = len(rows)
    done = sum(1 for r in rows if r.get("review_status") == "reviewed")
    print(f"Loaded {total} rows from {path} (reviewed: {done}/{total}).")
    idx = 0
    if start_id:
        for i, r in enumerate(rows):
            if r.get("audit_id") == start_id:
                idx = i
                break
    else:
        for i, r in enumerate(rows):
            if r.get("review_status") != "reviewed":
                idx = i
                break
    while 0 <= idx < total:
        row = rows[idx]
        _ar_show(audit_kind, row, idx, total)
        if row.get("review_status") == "reviewed":
            cmd = input("[m]odify, [n]ext pending, [p]revious, [j]ump, [q]uit: ").strip().lower()
            if cmd == "m":
                if not _ar_review_one(audit_kind, row):
                    break
                _ar_save(path, fields, rows)
            elif cmd == "n":
                nxt = next((i for i in range(idx + 1, total) if rows[i].get("review_status") != "reviewed"), None)
                idx = nxt if nxt is not None else total
            elif cmd == "p":
                idx = max(0, idx - 1)
            elif cmd == "j":
                target = input("audit_id: ").strip()
                idx = next((i for i, r in enumerate(rows) if r.get("audit_id") == target), idx)
            elif cmd == "q":
                break
            continue
        cmd = input("[a]ssign label, [s]kip, [p]revious, [j]ump, [q]uit: ").strip().lower()
        if cmd == "a":
            if not _ar_review_one(audit_kind, row):
                break
            _ar_save(path, fields, rows)
            nxt = next((i for i in range(idx + 1, total) if rows[i].get("review_status") != "reviewed"), None)
            idx = nxt if nxt is not None else total
        elif cmd == "s":
            idx += 1
        elif cmd == "p":
            idx = max(0, idx - 1)
        elif cmd == "j":
            target = input("audit_id: ").strip()
            idx = next((i for i, r in enumerate(rows) if r.get("audit_id") == target), idx)
        elif cmd == "q":
            break


# ---------------------------------------------------------------------------
# Unified CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="OSSGameBench Option A: Bug Resolution (unified replication script)"
    )
    ap.add_argument(
        "stage",
        choices=[
            "extract", "prepare", "scan", "build", "stats", "extended",
            "figures", "final_checks", "validation", "audit_eval",
            "audit_review", "all",
        ],
    )
    ap.add_argument("--force", action="store_true", help="Force re-extraction, commit rescan, or audit regeneration")
    ap.add_argument("--audit", choices=["classification", "bug", "all"], default="all",
                    help="Audit target for audit_eval / audit_review")
    ap.add_argument("--start", default=None, help="Starting audit_id for audit_review")
    args = ap.parse_args()

    if args.stage in ("extract", "all"):
        stage_extract(force=args.force)
    if args.stage in ("prepare", "all"):
        stage_prepare(force=args.force)
    if args.stage in ("scan", "all"):
        stage_scan(force=args.force)
    if args.stage in ("build", "all"):
        stage_build(force=args.force)
    if args.stage in ("stats", "all"):
        stage_stats(force=args.force)
    if args.stage in ("extended", "all"):
        stage_extended()
    if args.stage in ("figures", "all"):
        stage_figures()
    if args.stage in ("final_checks", "all"):
        stage_final_checks()
    if args.stage in ("validation", "all"):
        stage_validation(force=args.force)
    if args.stage in ("audit_eval", "all"):
        stage_audit_eval(audit_kind=args.audit)
    if args.stage == "audit_review":
        if args.audit == "all":
            ap.error("audit_review requires --audit classification or --audit bug")
        stage_audit_review(args.audit, start_id=args.start)
