# Releasing Addax

This document describes how to cut a release. It exists so that anyone with write access to the repository can ship a version without reverse-engineering the build pipeline.

## Release pipeline overview

Releases are triggered **by pushing a version tag** (e.g. `6.0.13`). The tag pushes start the [`maven-publish.yml`](.github/workflows/maven-publish.yml) workflow, which performs, in order:

1. Build the distribution package (`mvn clean package` + `mvn package -Pdistribution` + `shrink_package.sh`)
2. Generate a changelog from git log since the previous tag
3. Create a GitHub Release with the tarball and its `sha256sum` file
4. Build and push Docker images to Docker Hub and Quay.io (`:<version>` and `:latest`)
5. Deploy artifacts to Maven Central (`mvn deploy -DautoPublish=true -DwaitUntil=PUBLISHED`)

**Important:** the CI workflow runs `mvn deploy` itself. As a maintainer you **never** run `mvn release:perform` — you only run `mvn release:prepare` locally, then push the tag.

## Prerequisites

- JDK 17 (Temurin) and Maven 3.8.8+ on the release machine
- Write access to `wgzhao/Addax`
- The following repository secrets must exist for the workflow to publish (check them in **Settings → Secrets and variables → Actions** before releasing):
  - `OSSRH_USERNAME`, `OSSRH_PASSWORD` — Sonatype/Maven Central credentials
  - `GPG_PRIVATE_KEY`, `GPG_PASS_PHRASE` — signing key for Maven Central
  - `DOCKER_USERNAME`, `DOCKER_PASSWORD` — Docker Hub
  - `QUAY_USERNAME`, `QUAY_PASSWORD` — Quay.io

## Standard release steps

A release is a "maintenance release": dependency/CVE updates and bug fixes, no new features (version scheme `x.y.z` is documented in the README).

1. **Make sure `master` is green and in sync.**
   ```
   git checkout master && git pull origin master
   ```
   The last commit should be intentional — do not release with unrelated unmerged work on the branch.

2. **Prepare the release with maven-release-plugin** (batch mode, no auto-push — pushes are done manually in step 3):
   ```
   mvn release:prepare -B -DpushChanges=false \
       -DreleaseVersion=6.0.13 -DdevelopmentVersion=6.0.14-SNAPSHOT
   ```
   This creates:
   - commit `[maven-release-plugin] prepare release 6.0.13` (version bumped to release version)
   - commit `[maven-release-plugin] prepare for next development iteration` (version bumped to next SNAPSHOT)
   - tag `6.0.13` (`tagNameFormat` is `@{project.version}`, no `v` prefix)

   **Verify the tag before pushing** (a stale `release.properties` from a failed run can silently tag the wrong tree):
   ```
   git show 6.0.13:pom.xml | grep -m1 '<version>'   # must be 6.0.13, not -SNAPSHOT
   git log --oneline 6.0.13 -2                       # must show the prepare-release commit
   ```

3. **Push the branch and the tag.**
   ```
   git push origin master
   git push origin 6.0.13
   ```
   The tag push starts the release workflow — follow it at **Actions → Maven Package**. Note the tag content is what gets built: if a fix is needed after the tag is pushed, cherry-pick it onto the release commit, `git tag -f 6.0.13`, delete the remote tag and push it again.

4. **Verify the release.** Check each artifact after the workflow finishes:
   - [GitHub Release](https://github.com/wgzhao/Addax/releases) exists with `addax-<version>.tar.gz` and `.sha256sum.asc` assets
   - Docker images: `docker pull <docker-username>/addax:<version>` and `quay.io/<quay-username>/addax:<version>` (also confirm `:latest` was updated)
   - Maven Central: artifacts are published under the `com.wgzhao.addax` groupId to `central.sonatype.com` (see the `distributionManagement` section of `pom.xml`)

## Troubleshooting

| Symptom | Likely cause / fix |
| --- | --- |
| `release:prepare` fails: tag already exists | A tag for this version already exists (e.g. a previous failed attempt). Delete the tag with `git tag -d <version>` and `git push origin :<version>` **only if you are sure it was never released**, then re-run. |
| Workflow fails at Maven Central step | Sonatype staging issue or GPG signing problem. Re-run the failed job from the Actions page first; if it persists, verify `GPG_PRIVATE_KEY`/`GPG_PASS_PHRASE` and `OSSRH_*` secrets. Staging can also be dropped/confirmed manually at [s01.oss.sonatype.org](https://s01.oss.sonatype.org). |
| Docker push fails | Registry credentials expired or rotated — update `DOCKER_USERNAME`/`DOCKER_PASSWORD` or `QUAY_USERNAME`/`QUAY_PASSWORD` secrets. |
| Changelog in the GitHub Release looks wrong | The workflow diffs `git log` between the latest release tag and the new tag. If a tag was created out of order (e.g. a hotfix tag), the changelog may be off — it can be edited on the GitHub Release page after the fact. |
| `release:prepare` fails at the push step | The run may have already pushed the tag (and left remote pollution) before failing on master. Before retrying: delete the remote tag if it exists (`git ls-remote origin <version>`), `git reset --hard origin/master`, and **delete the leftover `release.properties`** — a stale one makes the next `prepare` run skip the release commit and tag the wrong tree. |
| Tag tree has the wrong version | After `prepare`, verify before pushing: `git show <version>:pom.xml` must contain the release version (not `-SNAPSHOT`), and `git log <version>` must contain the `prepare release` commit. If not, clean up and re-run (see previous row). |
| Maven Central step fails: `gpg: signing failed: No pinentry` | The generated `settings.xml` contains an active gpg profile that redirects the passphrase env name (`gpg.passphraseEnvName`). The deploy step env must export **both** `GPG_PASSPHRASE` and `MAVEN_GPG_PASSPHRASE` (see `maven-publish.yml`), and maven-gpg-plugin must be ≥ 3.2.0 for setup-java v6. |
| Maven Central step fails: `server is null` (central-publishing) | The deploy step must pass `-s "${{ github.workspace }}/settings.xml"` — setup-java writes the settings there, but maven only reads `~/.m2/settings.xml` by default — and the step env must define `MAVEN_USERNAME`/`MAVEN_PASSWORD` for the `${env.X}` placeholders in that file. |

## Maintenance vs. hotfix releases

- **Maintenance release** (the normal case): cut from `master` when enough fixes/dependency updates have accumulated, roughly monthly.
- **Hotfix release**: for a serious bug or security fix, cut a new patch version from the current `master` ahead of schedule — same procedure, just earlier. Do not branch from old tags for this; the repo has no backport flow, so `master` is the only release line.
