<!--
Please fill in the sections below when opening a pull request. This template is
based on GitHub community best practices and adapted for this project.
-->

<!-- Title -->
<!-- Use a short, descriptive title: e.g. "[FIX] Avoid invalid BSON field names in MongoDB writer" -->

## Summary
<!-- Provide a short description of what this PR changes and why. -->


## Related Issue(s)
<!-- Link to any existing issues this PR addresses: Fixes #123, Relates to #456 -->


## What type of change is this?
<!-- Check one or more boxes: -->
- [ ] Bugfix
- [ ] New feature
- [ ] Performance improvement
- [ ] Documentation
- [ ] Tests
- [ ] Build / CI
- [ ] Chore / Refactor
- [ ] Breaking change (changes API or runtime behavior)


## Changes
<!-- Describe the main changes in this PR. Keep each change short and focused. -->


## Motivation and Context
<!-- Why is this change required? What problem does it solve? -->


## How has this been tested?
Please include steps to reproduce and verify the changes locally. Examples:

- Automated tests added: `module/src/test/...` (describe)
- Manual steps:
  1. Build project: `mvn -DskipTests install` (from repo root)
  2. Run writer module build: `mvn -DskipTests package -f plugin/writer/mongodbwriter/pom.xml`
  3. Start a local MongoDB and run the job (or use the provided test harness)
  4. Verify the document written contains nested field and array as expected

Include relevant logs or small snippets if helpful.


## Checklist (required)
- [ ] I have read the project's contributing guidelines and code of conduct.
- [ ] My change includes appropriate tests (if applicable).
- [ ] I ran a local build and fixed any compilation issues.
- [ ] I updated or added documentation if needed (README, docs/ or docs/en/).
- [ ] I updated CHANGELOG.md when appropriate.
- [ ] I have not added any secrets or credentials.
- [ ] I have checked for sensitive or personal data in this PR.
- [ ] I added the `Signed-off-by` line in the final commit message if required by the project.


## Checklist for maintainers / reviewers (recommended)
- [ ] Verify the implementation follows project conventions and style.
- [ ] Confirm tests (unit/integration) and CI pass.
- [ ] Check release notes / CHANGELOG entry.
- [ ] Evaluate whether the change requires a migration note.


## Release notes
<!-- Optional: short note that will be included in release notes. -->


## Breaking changes / Migration
<!-- If this PR introduces breaking changes, describe the impact and recommended migration steps. -->


## Security
If this PR fixes a security vulnerability, DO NOT include exploit details in this public description.
Instead, contact the maintainers by email or follow the project's security disclosure process.


## Additional context
<!-- Add any other context or screenshots about the PR here. -->


---

<!-- Guidance for PR authors: keep the template short and actionable. -->

*Tips:*
- Use a concise, prefixed title like `[FIX]`, `[FEATURE]`, `[DOC]`, or `[BREAKING]`.
- Group related changes into a single logical PR where possible.
- If the change affects end-users, add a clear `Release notes` entry and a migration paragraph.
- For large or risky changes, include a short test plan and a rollback plan.

