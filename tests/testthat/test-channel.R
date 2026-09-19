test_that("values round-trip in FIFO order", {
  ch <- mizu_inproc_channel(2)
  mizu_send(ch, "a")
  mizu_send(ch, 42)
  expect_identical(mizu_recv(ch), "a")
  expect_identical(mizu_recv(ch), 42)
})

test_that("NULL is a legal payload", {
  ch <- mizu_inproc_channel(1)
  mizu_send(ch, NULL)
  expect_null(mizu_recv(ch))
})

test_that("the ring wraps", {
  ch <- mizu_inproc_channel(2)
  for (i in 1:5) {
    mizu_send(ch, i)
    expect_identical(mizu_recv(ch), i)
  }
  mizu_send(ch, 1)
  mizu_send(ch, 2)
  expect_identical(mizu_send(ch, 3), mizu_full)
  expect_identical(mizu_recv(ch), 1)
  mizu_send(ch, 3)
  expect_identical(mizu_recv(ch), 2)
  expect_identical(mizu_recv(ch), 3)
})

test_that("a large ring preserves order and integrity", {
  n <- 10000L
  ch <- mizu_inproc_channel(n)
  for (i in seq_len(n)) {
    mizu_send(ch, i)
  }
  expect_identical(mizu_send(ch, n + 1L), mizu_full)
  expect_identical(replicate(n, mizu_recv(ch)), seq_len(n))
})

test_that("the buffer survives garbage collection", {
  ch <- mizu_inproc_channel(2)
  mizu_send(ch, "a")
  gc()
  expect_identical(mizu_recv(ch), "a")
})

test_that("send on a full channel returns mizu_full without waiting", {
  ch <- mizu_inproc_channel(1)
  expect_identical(mizu_send(ch, "a"), TRUE)
  expect_identical(mizu_send(ch, "b"), mizu_full)
  expect_invisible(mizu_send(ch, "c"))
})

test_that("recv on an empty channel returns mizu_timeout", {
  ch <- mizu_inproc_channel(1)
  expect_identical(mizu_recv(ch, timeout = 0), mizu_timeout)
  expect_identical(mizu_recv(ch, timeout = 0.01), mizu_timeout)
})

test_that("the timeout is respected", {
  ch <- mizu_inproc_channel(1)
  # wall-clock measurement of a monotonic bound: allow clock slew slack
  elapsed <- system.time(mizu_recv(ch, timeout = 0.05))[[3L]]
  expect_gte(elapsed, 0.04)
})

test_that("close drains buffered values, then returns mizu_closed", {
  ch <- mizu_inproc_channel(2)
  mizu_send(ch, "a")
  expect_identical(mizu_inproc_close(ch), TRUE)
  expect_identical(mizu_recv(ch), "a")
  expect_identical(mizu_recv(ch), mizu_closed)
  expect_identical(mizu_send(ch, "b"), mizu_closed)
  expect_identical(mizu_inproc_close(ch), TRUE)
})

test_that("mizu_is_sentinel is an identity test", {
  expect_identical(mizu_is_sentinel(mizu_full), TRUE)
  expect_identical(mizu_is_sentinel(mizu_timeout), TRUE)
  expect_identical(mizu_is_sentinel(mizu_closed), TRUE)
  expect_identical(mizu_is_sentinel("a"), FALSE)
  expect_identical(mizu_is_sentinel(NULL), FALSE)
  expect_identical(
    mizu_is_sentinel(
      structure("x", class = c("mizu_timeout", "mizu_sentinel"))
    ),
    FALSE
  )
})

test_that("a forwarded sentinel arrives as an ordinary copy", {
  ch <- mizu_inproc_channel(1)
  mizu_send(ch, mizu_timeout)
  x <- mizu_recv(ch)
  expect_identical(inherits(x, "mizu_sentinel"), TRUE)
  expect_identical(mizu_is_sentinel(x), FALSE)
})

test_that("a sentinel payload drains before the closed sentinel", {
  ch <- mizu_inproc_channel(1)
  mizu_send(ch, mizu_full)
  mizu_inproc_close(ch)
  x <- mizu_recv(ch)
  expect_identical(inherits(x, "mizu_sentinel"), TRUE)
  expect_identical(mizu_is_sentinel(x), FALSE)
  expect_identical(mizu_recv(ch), mizu_closed)
})

test_that("a channel prints its state and buffer occupancy", {
  ch <- mizu_inproc_channel(2)
  expect_snapshot(print(ch))
  mizu_send(ch, "a")
  expect_snapshot(print(ch))
  mizu_inproc_close(ch)
  expect_snapshot(print(ch))
  mizu_recv(ch)
  expect_snapshot(print(ch))
})

test_that("sentinels print as their class", {
  expect_snapshot(print(mizu_full))
  expect_snapshot(print(mizu_timeout))
  expect_snapshot(print(mizu_closed))
})

test_that("print methods return their input invisibly", {
  ch <- mizu_inproc_channel(1)
  capture.output(res <- withVisible(print(ch)))
  expect_identical(res$value, ch)
  expect_identical(res$visible, FALSE)
  capture.output(res <- withVisible(print(mizu_full)))
  expect_identical(res$value, mizu_full)
  expect_identical(res$visible, FALSE)
})

test_that("misuse is a plain error", {
  expect_snapshot(error = TRUE, mizu_inproc_channel(0))
  expect_snapshot(error = TRUE, mizu_inproc_channel(1.5))
  expect_snapshot(error = TRUE, mizu_recv("x"))
  expect_snapshot(
    error = TRUE,
    mizu_recv(mizu_inproc_channel(1), timeout = -1)
  )
})
