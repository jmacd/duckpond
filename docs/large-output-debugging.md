# Add large debugging

Duckpond uses `RUST_LOG=debug` or `RUST_LOG=tlogfs=debug` 

Datafusion uses `RUST_LOG=datafusion=debug`

Both of these may create large debugging outputs.

# How to work with large debugging outputs

Run the command with both standard output and standard error
redirected to a file. For example

```
RUST_LOG=debug \
  RUST_LOG=datafusion=debug
  POND=/tmp/pond \ 
  cargo run --bin pond show 1> OUT 2> OUT
```

Note that the command exit status is as-usual. You will know since you
ran the command whether it exited successfully or not.

This produces a file named `OUT`.

Now, read the file named `OUT`.

You can use tools such as `grep_search` to understand the output of
the test. 

The benefit of this approach is that you can search it again and
again, and we don't have to run the command again to study it in
detail.
