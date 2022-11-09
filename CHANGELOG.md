# CHANGELOG

### Version 4

Turned out that `datediff(millisecond, ...` can only return durations for
up to less than 4 weeks, causing some problems. Turning it into `datediff_big`.

Diagnosing this was difficult because the `insert-exec` together with `try-catch-rollback`
ended up obscuring the original error from logs. We had this problem during
release 3 too.  To avoid this in the future, we _copied_ the code in
`sweep()` into `sweep_loop()` instead of doing a procedure call. This seemed
like overall the cleanest approach towards users (in MSSQL, you can't
return tables from functions and any other approach would somehow be more
invasive.

To compensate (and be able to test), if you pass `duration=0` to `sweep_loop()`
it should always do exactly 1 iteration. Perhaps in the future we just
remove `sweep()` from the library and require that you use `sweep_loop(duration=0)`
instead, but keeping it for now.


### Version 3

Fixed a bug in a join in `sweep()`