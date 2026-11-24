# Asset-related bug fixes and issue closure time in open-source games

Replication package for the study:

> **Do Asset-Related Bug Fixes Have Longer Issue Closure Times Than Code-Only
> Fixes in Open-Source Games?**

The study uses OSSGameBench (MSR 2026) and does **not** repeat the original
paper's analyses. OSSGameBench describes the dataset and reports four
descriptive analyses (programming languages, game genres, game engines, and
label/commit counts). It contains no analysis of issue closure time or of
asset-versus-code bug fixes.

## 1. Data version and provenance

- Dataset: **OSSGameBench: A Large-Scale Dataset of Development Activities in
  Open-Source Video Games**, Marsad and Weeraddana, MSR 2026 Data and Tool
  Showcase Track, DOI `10.1145/3793302.3793313`.
- Archive used: `OSSGameBench.zip` (61.3 GB compressed, 76.5 GB uncompressed),
  Zenodo record `18359123`.
- The raw archive contains 970 project entries (including 20 exact duplicate
  renamed/transferred repositories) and 326,182 issue rows. `stage_prepare`
  automatically deduplicates those 20 renamed aliases, leaving **950 projects**
  and **319,709 issues**, matching Marsad and Weeraddana (2026) exactly.

The archive's parquet files were written with Arrow 21. Reading them with
older pyarrow versions fails with `Repetition level histogram size mismatch`.
`requirements.txt` therefore requires `pyarrow>=21`.

## 2. Research questions

- **RQ1.** Do bug reports with different modification types differ in their
  issue closure-time distributions?
- **RQ2.** After controlling for project differences, creation year, and
  modification size, is asset-related modification still associated with
  closure time?
- **RQ3.** Do mixed code-and-asset fixes show different closure-time
  characteristics?

## 3. Repository layout

```
OSSGameBench.zip              source archive (61.3 GB, Zenodo 18359123)
ossgamebench_analysis.py      unified end-to-end analysis, validation, and audit CLI
requirements.txt              Python dependencies
README.md                     reproduction instructions (this file)
report.docx / report.md       concise 4-page research report (Word & Markdown)
presentation.pptx             6-slide presentation deck
data/                         extracted main parquet tables
  issue.parquet
  pr.parquet
  issue_pr.parquet
  pr_commit.parquet
  project.parquet
  commit_metadata.parquet
results/                      cleaned data and statistical outputs
  analysis_base.parquet       closed non-PR issue universe with bug flags
  bug_issue_links.parquet     validated issue-PR links
  relevant_shas.parquet       commits that must be looked up
  commit_files_raw.parquet    changed files for linked commits
  analysis_dataset.csv        all bug reports, with group labels
  analysis_sample.csv         known-group analysis sample
  table1_descriptives.csv
  table2_pairwise_tests.csv
  table3_regression.csv         legacy four-model set (retained)
  table4_project_paired.csv
  table5_sensitivity.csv
  table6_leave_one_project_out.csv
  table7_stepwise_models.csv    S1-S5 stepwise models
  table8_transform_sensitivity.csv  log1p versus log
  table9_asset_project_sensitivity.csv
  table9b_asset_sensitivity_summary.csv
  table10_coverage_funnel.csv   issue/PR/commit/project denominators
  table11_paired_reconciliation.csv
  table12_sample_loss.csv       full issue reconciliation
  table13_selection_cohorts.csv
  table14_selection_features.csv
  table15_selection_projects.csv
  table16_selection_closure.csv
  table17_size_distribution.csv
  table18_size_common_support.csv
  table19_size_stratified.csv
  table20_size_restricted_models.csv
  table21_complete_recovery_check.csv  per-issue 100% commit-recovery check & S5 refit
  table_bootstrap.csv
  table_effective_sample.csv
  table_linkage_bias.csv
  table_sample_flow.csv
  kruskal_wallis.txt
  model_diagnostics.txt
  project_group_summary.csv
  project_paired_data.csv
  bootstrap_replicates.npz
  figures/figure1_sample_flow.png|.pdf
  figures/figure2_group_counts.png|.pdf
  figures/figure3_closure_boxplot.png|.pdf
  figures/figure4_project_differences.png|.pdf
  figures/figure5_effect_estimates.png|.pdf
  figures/figure6_modification_size.png|.pdf
validation/                   audit samples and quality checks
  classification_audit_sample.csv
  bug_identification_audit_sample.csv
  audit_protocol.md
  audit_status.json
  bug_identification_rules.md
  classification_taxonomy.md
  data_quality_checks.csv|.json
  linkage_coverage.csv
  commit_coverage.csv
  commit_coverage_summary.csv
  coverage_definitions.md
  selection_bias.md
  common_support.md
  audit_evaluation.json|.md
  project_concentration.csv
  bug_label_examples.csv
reference/                    paper, appendix, official scripts/notebooks
```

## 4. Reproduction

### 4.1 Environment

Python 3.11+ is required; the reported run used Python 3.13 on Windows 11.
From the repository root directory:

```bash
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
# Linux / macOS:     source .venv/bin/activate
pip install -r requirements.txt
```

By default, the pipeline looks for `OSSGameBench.zip` in the repository root
and extracts the six main parquet tables to `./data/`. To point to another
location, set the optional environment variables `OSS_ZIP` and `OSS_DATA_DIR`.

### 4.2 Full run (recommended single-file script)

All analysis, selection-bias, common-support, and audit-evaluation stages are
combined in a single self-contained script `ossgamebench_analysis.py`:

```bash
python ossgamebench_analysis.py all
```

This runs, in order:

1. `extract` - extracts the six main parquet tables from `OSSGameBench.zip`
   into `data/` (skipped when present with the expected size).
2. `prepare` - deduplicates the 20 renamed repositories (970 -> 950 projects;
   326,182 -> 319,709 issues), identifies bug reports, and validates links.
3. `scan` - streams all per-project commit parquet files from the archive,
   keeps only linked commits, and writes `results/commit_files_raw.parquet`.
   The scan reads 76.5 GB and took about 9 minutes on the reported machine.
4. `build` - classifies changed files and builds the issue-level sample.
5. `stats` - descriptive statistics, tests, cluster bootstrap, regressions,
   paired analyses, sensitivity and leave-one-project-out analyses.
6. `extended` - writes the stepwise models, transform check, asset project
   sensitivity, paired reconciliation, coverage funnel, and 100% commit-recovery
   completeness check (`table21_complete_recovery_check.csv`).
7. `figures` - renders Figures 1-5 as PNG and PDF.
8. `final_checks` - writes Tables 13-20, Figure 6, `validation/selection_bias.md`
   and `validation/common_support.md`.
9. `validation` - generates stratified audit samples (automatically preserves
   existing reviewed audit CSVs unless `--force` is passed).
10. `audit_eval` - computes inverse-stratum-weighted metrics and exact
    Clopper-Pearson stratum 95% CIs.

Individual stages can also be run directly (e.g., `python ossgamebench_analysis.py stats`,
`python ossgamebench_analysis.py audit_review --audit classification`, or
`python ossgamebench_analysis.py scan --force` to force a clean rescan).

### 4.3 Runtime and storage

- Main tables: about 340 MB on disk.
- Commit scan: about 9 minutes, 158k output file rows, temporary space equal to
  the largest single commit parquet file (about 8.6 GB for one project).
- Statistics, figures, validation and final checks: under two minutes once the
  scan exists.
- Total free disk needed: at least 15 GB beyond the archive.

## 5. Measurement and classification

### 5.1 Closure time

```
closure_days = (closed_at - created_at) / 86400
```

This is the recorded time from issue creation to the issue's recorded closure.
It includes triage, prioritization, waiting, release cadence and possibly
stale/duplicate closure. It is **not** debugging time, effort, or proof of a
successful fix.

### 5.2 Bug identification

A closed, non-PR issue is a bug report when either rule fires:

- **Label rule.** A label matches `bug`, `bugs`, `bugfix`, `bug fix`,
  `bug report`, `defect(s)`, `regression`, `crash`, `crash or hang`, or
  `error report`; labels expressing `not a bug`, `feature request`,
  `enhancement`, `question`, `discussion`, `suggestion`, `idea`, or
  `documentation` veto the rule.
- **Conservative text rule.** The title contains a defect term **and** the body
  contains a reproduction/report marker (for example, "steps to reproduce",
  "expected behavior", "actual behavior", "stack trace"), or the title begins
  with `[Bug]`, `Bug:`, `[Crash]`, etc.

Counts (after deduplicating the 20 renamed repositories): 72,385 closed bug
reports by label, 10,185 additional text-only reports, 82,570 closed bug reports
in total (95,267 including 12,697 open bug reports). The label-only and
text-positive definitions are sensitivity analyses.

### 5.3 Issue-PR-commit linkage

Only verifiable links are used:

- `issue_pr.parquet`, which the dataset builds from GitHub's
  `closingIssuesReferences` and explicit "fixes/closes/resolves #N" patterns;
- `pr_commit.parquet`, GitHub's PR-to-commit mapping.

No temporal proximity, file-name similarity, or author matching is used.
Links to missing PRs (137) or to issue numbers outside the cleaned issue table
(6,116) are dropped. Only merged PRs with at least one commit are used.
Duplicate issue-PR pairs are removed.

Because PR head commits are not always reachable from the default branch of the
cloned repository, **18,339 of 55,940** wanted PR-commit records (32.8%) were
found in the per-project commit files. This is **commit-record recovery**, not
issue coverage. Issue coverage is 23.6% with a merged linked PR, 9.0% with
recovered files, and 8.7% in the known-group sample. PR coverage is 34.7% with
recovered files. Within the 7,155 analyzed issues, **7,042 (98.4%)** have 100%
of their linked PR commits recovered (`table21_complete_recovery_check.csv`).
See `results/table10_coverage_funnel.csv` and `validation/coverage_definitions.md`;
approximate matching is not used.

### 5.4 File classification

Every changed path is classified as CODE, ASSET or OTHER.

- **CODE:** source and scripting languages, shaders, HTML/CSS, build files,
  engine scripts.
- **ASSET:** images, audio, video, 3-D/2-D models, animation, fonts, tile maps,
  atlases, Unity/Godot scene, prefab and material assets, and authored map
  files (`.vmf`, `.bsp`, `.oramap`).
- **OTHER:** documentation, localization, configuration, schemas, package
  manifests, binary libraries, archives and unknown extensions.

Context overrides prevent common false positives: images under `docs/`,
`documentation/`, `fastlane/`, `dist/`, or test/fixture directories are OTHER;
Unity `ProjectSettings/` and settings assets are OTHER; dependency/metadata
basenames such as `go.mod`, `go.sum`, `copyright`, and `license` are OTHER
(preventing `go.mod` from matching tracker audio `.mod`); package archives are
not assets. The full taxonomy is in `validation/classification_taxonomy.md`.

Issue-level groups use the union of files from all merged linked PRs:

| Group | Rule |
|---|---|
| `code_only` | at least one CODE, no ASSET |
| `asset_only` | at least one ASSET, no CODE |
| `mixed` | both CODE and ASSET |
| `other` | files recovered, but only OTHER |
| `unknown` | no file recovered |

OTHER files do not by themselves change the CODE/ASSET contrast (a code fix
that also edits a changelog remains code-only). This rule is audited in
`classification_audit_sample.csv`.

### 5.5 Multiple PRs and edge cases

- Primary rule: union of all merged linked PRs that have recovered files.
- Sensitivity: largest PR only; earliest merged PR only; single-PR issues only.
- Multiple issues closed by the same PR remain separate issue observations;
  project-clustered inference is used.
- Issues closed before the first linked merge are retained in the primary
  sample and removed in a sensitivity analysis.
- Open issues, missing `closed_at`, negative durations and issue records that
  are PRs are excluded.

## 6. Statistical analysis

- Descriptive statistics: n, percentage, mean, SD, median, Q1, Q3, IQR, P90,
  P95, minimum and maximum.
- Omnibus test: Kruskal-Wallis.
- Pairwise tests: two-sided Mann-Whitney U with Benjamini-Hochberg correction;
  Cliff's delta as effect size.
- Confidence intervals: project-cluster bootstrap (2,000 replicates), resampling
  projects with replacement and keeping all issues within a project. This
  addresses within-project dependence.
- Regression: OLS on `log1p(closure_days)` with project-clustered robust
  standard errors. Stepwise sequence: S1 type only; S2 + creation year; S3 +
  project fixed effects; S4 + `log1p(n_files)`; S5 all controls. A legacy
  four-model table is retained in `table3_regression.csv`.
- Transform check: the full model is refitted with `log(closure_days)`; its
  exponentiated coefficient is a ratio of conditional geometric mean days.
- Overadjustment check: S3 (year + project FE, no size) and S5 (full) are both
  reported; `n_files` may be a consequence rather than a confounder of mixed
  changes.
- Within-project comparison: projects with at least three issues in both
  groups; Wilcoxon signed-rank and sign test on project median differences;
  stepwise models refitted on the paired sample.
- Sensitivity: label-only and text-only bug definitions, exclusion of closures
  over 365 days, winsorization at P99, exclusion of non-fix labels, single-PR
  issues, closure on/after merge, largest/earliest PR, exclusion of `<1-day`
  closures and bot-authored issues, project-balanced medians, exclusion of the
  dominant asset project, and leave-one-project-out full-model estimates.
- Coverage: issue, PR, commit and project denominators are reported separately
  in `table10_coverage_funnel.csv` and `validation/coverage_definitions.md`.

Regression coefficients are multiplicative shifts in `(closure_days + 1)`, not
ratios of raw closure days and not percent changes in days. Log models are an
approximation; the nonparametric tests and bootstrap do not assume normality.

## 7. Classification and bug-identification validation

Two stratified audit samples are provided and evaluated with inverse-stratum
weights and 2,000-replicate bootstrap 95% CIs:

- `validation/classification_audit_sample.csv`: 180 issues stratified as 50
  code-only, 30 asset-only, 50 mixed, 25 other and 25 unknown.
- `validation/bug_identification_audit_sample.csv`: 80 issues in four strata
  (20 label+text, 20 label only, 20 text only, 20 neither/non-bug controls).

**Disclosure of reviewer type:** All 180 classification rows and 80
bug-identification rows were audited by an **independent LLM blinded to the
automated pipeline labels** (`reviewer_type = independent_llm_blinded` in
`validation/audit_status.json`, with `[LLM audit]` notes on every row). This is
an LLM-based validation rather than human dual-coding. Weighted evaluation
(`python ossgamebench_analysis.py audit_eval`) yields (with exact
Clopper-Pearson 95% CIs for individual stratum precision):

- **Classification audit (180/180):** weighted accuracy `0.999` (unweighted
  diagnostic `0.972`, 175/180); `code_only` precision `50/50` (`1.000`, exact
  95% CI `[0.929, 1.000]`), weighted recall `0.993` `[0.987, 0.999]`;
  `asset_only` precision `30/30` (`1.000`, exact `[0.884, 1.000]`); `mixed`
  precision `50/50` (`1.000`, exact `[0.929, 1.000]`); `other` precision
  `20/25` (`0.800`, exact `[0.593, 0.932]`; 5/25 `other` cases were
  domain-specific game scripts or UI XML judged as `code_only`); `unknown`
  precision `25/25` (`1.000`, exact `[0.863, 1.000]`).
- **Bug-identification audit (80/80):** weighted bug precision `0.982`
  `[0.963, 1.000]`, weighted bug recall `0.532` `[0.428, 0.695]`, weighted
  accuracy `0.721` (unweighted `0.863`, 69/80). Label-based strata reached
  40/40 (100%), text-only 17/20 (85%), and 8/20 `neither` controls were
  unlabeled genuine bugs missed by the conservative rule.

Human reviewers can independently re-audit or override rows locally using:

```powershell
python ossgamebench_analysis.py audit_review --audit classification
python ossgamebench_analysis.py audit_review --audit bug
python ossgamebench_analysis.py audit_eval
```

The `audit_review` subcommand saves after each reviewed row and can be resumed.
The `audit_eval` subcommand uses inverse-stratum weights, 2,000 bootstrap
replicates, and exact Clopper-Pearson binomial intervals (`audit_protocol.md`).

## 8. Key outputs

- `results/analysis_sample.csv`: 7,155 known-group bug reports across 275
  deduplicated projects (6,662 code-only, 69 asset-only, 424 mixed).
- `results/table21_complete_recovery_check.csv`: per-issue commit recovery
  completeness (7,042 / 7,155 = 98.4% complete) and complete-recovery S5 refit.
- `results/table1_descriptives.csv`: closure-time distributions.
- `results/table2_pairwise_tests.csv`: Mann-Whitney, Cliff's delta, BH-adjusted
  p-values, bootstrap CIs.
- `results/table3_regression.csv`: legacy four-model estimates (retained).
- `results/table4_project_paired.csv`: within-project paired tests.
- `results/table5_sensitivity.csv`: 14 sensitivity scenarios.
- `results/table6_leave_one_project_out.csv`: earlier project-influence checks.
- `results/table7_stepwise_models.csv`: S1-S5 stepwise models.
- `results/table8_transform_sensitivity.csv`: `log1p` versus `log`.
- `results/table9_asset_project_sensitivity.csv` and
  `table9b_asset_sensitivity_summary.csv`: asset-only project influence.
- `results/table10_coverage_funnel.csv`: issue/PR/commit/project coverage.
- `results/table11_paired_reconciliation.csv`: paired-sample stepwise models.
- `results/table12_sample_loss.csv`: complete issue reconciliation.
- `results/table13_selection_cohorts.csv` to `table16_selection_closure.csv`:
  sample-selection cohorts, features, projects and closure-time comparison.
- `results/table17_size_distribution.csv` to
  `table20_size_restricted_models.csv`: modification-size distributions, common
  support, size bins and restricted models.
- `results/figures/figure6_modification_size.png`: size distributions.
- `results/figures/`: Figures 1-6 in PNG and PDF.
- `report.docx` / `report.md`: concise 4-page research report with
  interpretation and limitations.

## 9. Known limitations

- Only 32.8% of wanted PR commits were recovered (commit-record recovery), and
  only 8.7% of closed bug reports (7.5% of all bug reports) enter the known-group
  sample; 63,062 bug reports have no merged linked PR. The top ten projects
  contribute 62.3% of the included issues. Linked issues are newer and less often
  carry non-fix labels than unlinked issues (`table_linkage_bias.csv`). Within
  the analyzed sample, however, 98.4% of issues (7,042/7,155) have 100% of their
  linked commits recovered (`table21_complete_recovery_check.csv`).
- Code-only and mixed issues differ structurally in modification size: median
  2 versus 62 files, with only 28.8% of mixed issues in the central [4, 35]
  common-support range. Restricted full-model estimates change sign across
  support definitions.
- Asset-only cases are rare: 69 issues from 18 projects, 53 PRs and 87 commits;
  50.7% come from one project (`reactivedrop/reactivedrop_public_src`), where
  three PRs alone close 18 issues. Excluding that project reduces the full-model
  ratio from 1.80 (95% CI [0.996, 3.26], p = 0.051) to 1.65 (95% CI [0.87, 3.15],
  p = 0.127).
- Issue closure is not fix success. The dataset lacks `state_reason`, so
  duplicate, invalid, wontfix and stale closures cannot be fully separated.
- Open issues are right-censored and excluded.
- File classification uses paths, not content or diff semantics.
- Project fixed effects absorb between-project differences; they estimate
  within-project contrasts and can be underpowered for asset-only cases.
- The reported associations are not causal.
