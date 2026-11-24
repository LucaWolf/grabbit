# The connection notifier retry is interruptible

`Connection.manage` refreshes its AMQP notifier channels after every recovery. When
that refresh fails (the base connection is unset), the loop retries after a flat
`delayer.Delay(3)` pause. That pause used to be a bare `time.Sleep`, so a context
cancellation arriving in the window — `Close` before the first establishment, or a
cancel while the base connection is reset — could not reach the loop: the manage
goroutine slept and retried forever, leaking the goroutine and the latch watcher.

The pause is now the repository's own interruptible idiom, `delayerCompleted`: it
selects on `ctx.Done()` as well as the timer, so cancellation always exits the loop
(the same escape the channel twin already had through `recover` →
`reconnectLoop`). The branch keeps its retry-forever behavior while the context is
alive; only the abandonment path changed.

## Caller-driven abandonment

Recovery retries indefinitely by design. A caller abandons a loop through one of
three controls, never through a retry-count limit:

- **Callback veto** — `CallbackAllowedDown` (`WithConnectionOptionDown`) or
  `CallbackAllowedRecovery` (`WithConnectionOptionRecovering`) returns false.
- **Context cancellation** — the context passed to `WithConnectionOptionContext`
  (or the one `Close` cancels) terminates the loop at the next wait.
- **`Close`** — idempotent; cancels the context and clears the base connection.

No circuit breaker is added. A breaker would become a second authority competing
with these three over the same loop.

## Cadence as implemented

| Loop / pause | Attempts | Delay |
| --- | --- | --- |
| Connection reconnect (`connection.go`, `reconnectLoop`) | unbounded (counter wraps at `0xFFFF`) | saturating: `200ms * 2^(retry-1)`, exponent clamped by `RetryCap` (default 7, clamped 7–10) plus 10% jitter — ceiling 12.8s–102.4s |
| Connection notifier refresh (`connection.go`, `manage`) | unbounded while the context is alive | flat `Delay(3)` — 800ms plus jitter |
| Channel rebase pause (`channel.go`, `reconnectLoop`) | unbounded while the context is alive | flat `Delay(1)` — 200ms plus jitter |
| Channel notifier refresh (`channel.go`, `manage`) | bounded by the preceding `recover` call | flat `Delay(3)` — 800ms plus jitter |
| Anti-hot-loop timers (`connection.go`, `channel.go`) | every idle inner-loop pass | 120s |

The two in-repo statements that suggested a retry-count cap are corrected:
`NewDefaultDelayer`'s comment and the backoff bullet in `AGENTS.md` now say that
`RetryCap` clamps the delay exponent, not the attempts.

## Consequences

- Cancelling the context while the base connection is unset terminates the manage
  goroutine instead of leaking it; the suite-wide goleak harness can attribute any
  remaining leak to other work.
- The guard is scoped to the connection's notifier-failure path. The channel's
  notifier-failure pause stays a flat, non-interruptible `time.After`, but it is
  bounded (it follows a `recover` call that already observed the context), so it
  cannot spin forever.
- The flat `Delay(3)` / `Delay(1)` cadences are recorded as known behavior.
  Unifying them with the saturating connection policy is deliberate future work,
  not part of this guard.
