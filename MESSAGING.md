# Messaging Guide

Single source of truth for Addax's public copy. Read this before changing a description,
a tagline, or a README header.

## The rule

Every public sentence must be **derivable from the canonical description** below. If a new
sentence does not share keywords with it, it is a new positioning claim — decide that
deliberately, not by accident.

## Layer 1 — Canonical description

> Actively maintained successor to Alibaba DataX — a fast, versatile, open-source ETL tool for
> 30+ RDBMS and NoSQL data sources.

This is the most complete statement of what Addax is. Everything else is a subset of it.

Prefer **successor** over **fork**. Both are true, but "fork" frames Addax as a derivative and
leaves the reader wondering why they should not use the original; "successor" frames it as the
heir, which is the actual situation given DataX has been frozen since 2023.

| Surface | Where it lives |
| --- | --- |
| GitHub About | repo settings — the only surface not version-controlled |
| `pom.xml` | `<description>` |
| `Dockerfile` | `LABEL description` |
| `README.md` | opening paragraph under the badges |
| `README_zh.md` | paragraph below the badges |

## Layer 2 — Tagline

> Any source. Any target. Fast.

Three beats, each mapping to a claim Layer 1 already makes:

| Beat | Layer 1 claim it compresses |
| --- | --- |
| Any source / any target | versatile ... RDBMS/NoSQL |
| Fast | fast |

Constraints:

- Max 30 characters. It has to fit `logo-slogan.svg` at font-size 88 without wrapping.
- The rendered right edge must line up with the "Addax" wordmark (x ≈ 1131 on the 1280px canvas).
- Never introduce a claim Layer 1 does not make.

## Layer 3 — Chinese

> 任意数据源，任意目标，就是快

Use **数据集成** or **ETL**, never 数据采集 — collection is not extract-transform-load, and
mixing the two makes the Chinese copy describe a different product.

> Addax 是阿里开源 DataX 的活跃维护继任者 —— 一个快速、通用的开源 ETL 工具，支持 30+ 关系型与非关系型数据源。

## Words to avoid

| Word | Why |
| --- | --- |
| empower | Carries no information. True of every data product, so it distinguishes nothing. |
| comprehensive ... platform | Platform contradicts the tool positioning. Pick one. |
| ideal solution for X | Unverifiable claim. State what it does, let the reader judge. |

## Change checklist

Changing the tagline or the canonical description means updating **all** of these. A partial
update is what caused the drift this file exists to prevent.

- [ ] GitHub About — `gh repo edit wgzhao/Addax --description "..."`
- [ ] `logo-slogan.svg`, then re-render the social preview PNG and **re-upload it** in
      Settings → Social preview. Editing the SVG does not update the live preview.
- [ ] `README.md`
- [ ] `README_zh.md`
- [ ] `pom.xml`
- [ ] `Dockerfile`
- [ ] `CONTRIBUTING.md`
