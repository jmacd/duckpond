# Bug: `pond copy` lacks `--overwrite` support

## Summary

`pond copy host:///config/site /site` fails with `AlreadyExists` when the
destination directory already exists. There is no `--overwrite` flag on
`pond copy`, unlike `pond mknod` which supports `--overwrite` for idempotent
re-creation.

## Reproduction

```bash
# First copy succeeds
pond copy host:///config/site /site

# Second copy fails
pond copy host:///config/site /site
# Error: Recursive copy failed: TinyFS error: AlreadyExists: /site
```

## Expected Behavior

`pond copy --overwrite host:///config/site /site` should replace the
destination contents, similar to how `mknod --overwrite` works.

## Workaround

Currently there is no `pond rm` command either, so there is no way to
remove the existing directory before re-copying. The `update-remote.sh`
script skips the copy step and only runs `mknod --overwrite` for factory
nodes.

## Context

Found while building `water/update-remote.sh`. The septic `update.sh`
also uses `copy ... --overwrite` which suggests this may have worked in
an earlier version or the septic script has the same latent bug.
