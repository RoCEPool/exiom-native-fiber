# Upstream provenance

Target: https://github.com/RoCEPool/exiom-native-fiber (main).
Upstream: https://github.com/enjoy-digital/liteiclink (master).

Imported archive: liteiclink-master.zip, commit 1783beae6c0b433b1725accef5c11e526f8358eb.
Every archive file matched that upstream commit exactly.
Synchronized to 9ebb1cd86ded2424fc20dee3ba3c379ee25324ad on 2026-11-25.
The additional upstream commit fixes the README DeepWiki badge.

The upstream BSD-2-Clause LICENSE applies to the imported code.
INITIAL_LICENSE and INITIAL_README.md preserve the target repository's original
initialization files; INITIAL_LICENSE does not relicense upstream code.

Future synchronization (merge, never force push):

```sh
git remote add upstream https://github.com/enjoy-digital/liteiclink.git # once
git fetch upstream master
git merge upstream/master
git push origin main
```

The upstream CI and documentation checks run on pushes and pull requests.
Nightly documentation publishing is restricted to the upstream owner.
PyPI publishing is manual and requires separately configured trusted publishing.
No recurring synchronization has been configured.
