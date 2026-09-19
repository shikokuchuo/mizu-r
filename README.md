# mizu

Single-producer single-consumer channels for R: a bounded ring buffer
implemented in C, with optional timeouts and sentinel return values for
full, timeout, and closed states. The transport is in-process.

## Installation

```r
install.packages("mizu")
```

## Usage

```r
library(mizu)

ch <- mizu_inproc_channel(2)

mizu_send(ch, "a")
mizu_send(ch, 42)
mizu_send(ch, "b")          # full: returns the mizu_full sentinel

mizu_recv(ch)
#> [1] "a"
mizu_recv(ch)
#> [1] 42
mizu_recv(ch, timeout = 0)  # empty: do not wait
#> <mizu_timeout>

mizu_inproc_close(ch)
```

## License

MIT
