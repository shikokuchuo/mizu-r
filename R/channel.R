# Sentinels are interned singletons: classed environments, registered with
# the C core at load (.onLoad), so identity is a pointer test and a class
# look-alike payload can never forge one.
new_sentinel <- function(class) {
  structure(
    new.env(parent = emptyenv()),
    class = c(class, "mizu_sentinel")
  )
}

#' Sentinel Values
#'
#' Singletons returned by [mizu_send()] and [mizu_recv()] that
#' tag the terminal states of a channel as ordinary values, not signalled
#' conditions, so hot loops stay branch-cheap. Dispatch with `inherits(x,
#' "mizu_sentinel")` or on the specific class; use
#' [mizu_is_sentinel()] where payloads are untrusted.
#'
#' * `mizu_full` — the buffer is at capacity ([mizu_send()]).
#' * `mizu_timeout` — no value arrived within `timeout`
#'   ([mizu_recv()]).
#' * `mizu_closed` — the channel is closed and drained
#'   ([mizu_inproc_close()]).
mizu_full <- new_sentinel("mizu_full")

#' @rdname mizu_full
mizu_timeout <- new_sentinel("mizu_timeout")

#' @rdname mizu_full
mizu_closed <- new_sentinel("mizu_closed")

#' Create a Channel
#'
#' Creates a single-producer single-consumer channel: a bounded ring buffer
#' that holds up to `capacity` values until they are received. The ring
#' buffer is implemented in C, with O(1) sends and receives. The transport
#' is in-process — both ends of the channel live in the calling R process.
#'
#' This interface is experimental and may change in a future release.
#'
#' @param capacity a positive integer: the number of values the channel
#'   buffers before [mizu_send()] returns the [mizu_full]
#'   sentinel.
#'
#' @return A channel (class `"mizu_inproc_channel"`).
#'
#' @examples
#' ch <- mizu_inproc_channel(2)
#' mizu_send(ch, "a")
#' mizu_send(ch, 42)
#' mizu_recv(ch)
#' mizu_recv(ch)
#' mizu_inproc_close(ch)
#'
#' @useDynLib mizu, .registration = TRUE
#' @export
mizu_inproc_channel <- function(capacity) {
  check_capacity(capacity)
  structure(
    .Call(mizu_inproc_ring_create, capacity),
    class = "mizu_inproc_channel"
  )
}

#' Send and Receive over a Channel
#'
#' `mizu_send()` appends `x` to the buffer of the channel. Sends never
#' block: a send on a full channel returns the `mizu_full` sentinel
#' immediately. `mizu_recv()` removes and returns the oldest buffered
#' value, waiting up to `timeout` seconds for one to arrive.
#'
#' Terminal states surface as class-tagged sentinels, not errors. Dispatch
#' with `inherits(x, "mizu_sentinel")`, or on the specific classes:
#'
#' * `mizu_full` — the buffer is at capacity (send). Receive first, or
#'   drop the value.
#' * `mizu_timeout` — no value arrived within `timeout` (recv).
#' * `mizu_closed` — the channel was closed with
#'   [mizu_inproc_close()]. A receive drains all buffered values before it
#'   reports this.
#'
#' `NULL` is a legal payload. Sentinels are ordinary values, identifiable by
#' class alone, and never signalled conditions. [mizu_is_sentinel()]
#' checks identity where payloads are untrusted.
#'
#' @param ch a channel from [mizu_inproc_channel()].
#' @param x the payload: any R object.
#' @param timeout seconds to wait before the call returns the
#'   `mizu_timeout` sentinel. `Inf` (the default) waits indefinitely,
#'   and `0` polls. The wait is a bounded spin-then-backoff poll on a
#'   monotonic clock: system clock adjustments do not affect it, and Ctrl-C
#'   stays responsive throughout.
#'
#' @return `mizu_send()` returns `TRUE` (invisibly) on success, or a
#'   sentinel otherwise. `mizu_recv()` returns the received value or a
#'   sentinel.
#'
#' @examples
#' ch <- mizu_inproc_channel(1)
#' mizu_send(ch, "a")
#' mizu_send(ch, "b")  # full: returns the mizu_full sentinel
#' mizu_recv(ch)
#' mizu_recv(ch, timeout = 0)
#' mizu_inproc_close(ch)
#'
#' @export
mizu_send <- function(ch, x) {
  check_channel(ch)
  invisible(.Call(mizu_inproc_ring_send, ch, x))
}

#' @rdname mizu_send
#' @export
mizu_recv <- function(ch, timeout = Inf) {
  check_channel(ch)
  check_timeout(timeout)
  .Call(mizu_inproc_ring_recv, ch, timeout)
}

#' Close a Channel
#'
#' Orderly shutdown. Values buffered before the close remain receivable;
#' further sends return the `mizu_closed` sentinel, and a receive on a
#' drained channel returns the same. Closing an already-closed channel is a
#' no-op.
#'
#' @inheritParams mizu_send
#'
#' @return Invisibly, `TRUE`.
#'
#' @examples
#' ch <- mizu_inproc_channel(1)
#' mizu_send(ch, "a")
#' mizu_inproc_close(ch)
#' mizu_recv(ch)
#' mizu_recv(ch)
#'
#' @export
mizu_inproc_close <- function(ch) {
  check_channel(ch)
  invisible(.Call(mizu_inproc_ring_close, ch))
}

#' Test for a mizu Sentinel
#'
#' Identity comparison against the interned sentinel singletons —
#' [mizu_full], [mizu_timeout], and [mizu_closed] — that
#' the verbs of mizu return to tag terminal states. `inherits(x,
#' "mizu_sentinel")` tests the class alone, which any payload can
#' carry. This includes a genuine sentinel forwarded over a channel, which
#' arrives as an ordinary copy. `mizu_is_sentinel()` is provenance: `TRUE`
#' only for the exact objects that the own calls of mizu return. So code
#' that relays untrusted values can distinguish its terminal states from
#' look-alike payloads.
#'
#' Sentinels are ordinary values, not R conditions: nothing is signalled,
#' and condition handlers never see them.
#'
#' @param x any R object.
#'
#' @return `TRUE` or `FALSE`.
#'
#' @examples
#' ch <- mizu_inproc_channel(1)
#' x <- mizu_recv(ch, timeout = 0)
#' mizu_is_sentinel(x)
#' mizu_is_sentinel(42)
#' # class alone does not make a sentinel:
#' mizu_is_sentinel(
#'   structure("x", class = c("mizu_timeout", "mizu_sentinel"))
#' )
#' mizu_inproc_close(ch)
#'
#' @export
mizu_is_sentinel <- function(x) .Call(mizu_sentinel_check, x)

#' Print Methods for mizu Objects
#'
#' One-line summaries. A channel prints its state — `open` or `closed` —
#' and the number of buffered values out of its capacity. Sentinels print as
#' their class. The methods never error, so auto-printing is always safe.
#'
#' @param x the object.
#' @param ... ignored.
#'
#' @return `x`, invisibly.
#'
#' @examples
#' ch <- mizu_inproc_channel(2)
#' ch
#' mizu_send(ch, "a")
#' ch
#' mizu_inproc_close(ch)
#'
#' @export
print.mizu_inproc_channel <- function(x, ...) {
  st <- tryCatch(.Call(mizu_inproc_ring_stat, x), error = function(e) NULL)
  cat(
    if (is.null(st)) {
      "<mizu_inproc_channel: invalid>\n"
    } else {
      sprintf(
        "<mizu_inproc_channel: %s, %d/%d buffered>\n",
        if (st[3L] != 0L) "closed" else "open",
        st[1L],
        st[2L]
      )
    }
  )
  invisible(x)
}

#' @rdname print.mizu_inproc_channel
#' @export
print.mizu_sentinel <- function(x, ...) {
  cat(sprintf("<%s>\n", class(x)[1L]))
  invisible(x)
}

check_capacity <- function(capacity) {
  if (
    !is.numeric(capacity) ||
      length(capacity) != 1L ||
      is.na(capacity) ||
      capacity < 1 ||
      capacity > .Machine$integer.max ||
      capacity != trunc(capacity)
  ) {
    stop("mizu: capacity must be a positive integer", call. = FALSE)
  }
}

check_channel <- function(ch) {
  if (!inherits(ch, "mizu_inproc_channel")) {
    stop(
      "mizu: ch must be a channel created by mizu_inproc_channel()",
      call. = FALSE
    )
  }
}

check_timeout <- function(timeout) {
  if (
    !is.numeric(timeout) ||
      length(timeout) != 1L ||
      is.na(timeout) ||
      timeout < 0
  ) {
    stop("mizu: timeout must be a non-negative number", call. = FALSE)
  }
}

# Register the interned sentinels with the C core.
.onLoad <- function(libname, pkgname) {
  .Call(
    mizu_sentinels_init,
    mizu_full,
    mizu_timeout,
    mizu_closed
  )
}
