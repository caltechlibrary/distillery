# 1. Record architecture decisions

- Status: accepted
- Date: 2026-09-30

## Context and Problem Statement

Distillery's preservation design was built from a single root-level planning
document, `OCFL-PRESERVATION-PLAN.md`. It worked well while the work was in
flight and badly afterwards, for the reasons a plan always does: it mixed
durable rationale with a phase-by-phase build log, so a reader could not tell
which parts described a decision still in force and which described the route
taken to get there. Several passages had already been rewritten two or three
times as the design moved underneath them, and one section described
infrastructure that no longer existed.

Meanwhile the decisions themselves are the valuable part, and many of them are
only legible as the absence of something: `os.rename()` rather than
`shutil.move()`, a delimiter of `/` rather than the OCFL extension's default
`:`, a mount *point* rather than a mount *parent*. Each looks arbitrary, or
like an oversight, until you know what it is avoiding.

## Decision

Record architecturally significant decisions as ADRs in `docs/decisions/`,
following [MADR](https://adr.github.io/madr/), matching the convention
Alchemist adopted in its own ADR 0001.

Conventions:

- One file per decision, `NNNN-title-in-kebab-case.md`, numbered sequentially
  in the order recorded (not the order decided).
- Each has a status: `proposed`, `accepted`, `rejected`, `deprecated`, or
  `superseded by ADR-NNNN`.
- **ADRs are immutable once accepted.** A decision that changes is not edited;
  a new ADR supersedes it, and the old one is marked as superseded.
- Corrections of *fact* about what shipped are fine to amend in place, dated
  and noted. Changes of *decision* get a new ADR.
- Record the rejected options and why. That is usually the most valuable part
  and the part that cannot be recovered from the code.

`OCFL-PRESERVATION-PLAN.md` is removed rather than kept alongside these. Its
decisions are carried into ADRs 0002–0006; its build log, phase ordering and
progress notes are git history's job, and its two genuinely open questions are
now issues.

## Consequences

Design rationale gets a stable, linkable home, and code comments can cite a
specific decision (`ADR 0003`) instead of a section of a document that gets
renumbered whenever it is edited.

The cost is discipline: an ADR nobody writes is worthless, and one nobody
supersedes when the decision changes is worse than worthless. ADRs are for
decisions whose consequences outlive the change that introduced them — not a
log of every change, which is what `CHANGELOG.md` and git history are for.

Planning documents and build checklists do not belong in the repository. If a
plan produces a decision worth keeping, the decision graduates into an ADR;
the plan does not.
