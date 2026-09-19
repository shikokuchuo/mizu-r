# a channel prints its state and buffer occupancy

    Code
      print(ch)
    Output
      <mizu_inproc_channel: open, 0/2 buffered>

---

    Code
      print(ch)
    Output
      <mizu_inproc_channel: open, 1/2 buffered>

---

    Code
      print(ch)
    Output
      <mizu_inproc_channel: closed, 1/2 buffered>

---

    Code
      print(ch)
    Output
      <mizu_inproc_channel: closed, 0/2 buffered>

# sentinels print as their class

    Code
      print(mizu_full)
    Output
      <mizu_full>

---

    Code
      print(mizu_timeout)
    Output
      <mizu_timeout>

---

    Code
      print(mizu_closed)
    Output
      <mizu_closed>

# misuse is a plain error

    Code
      mizu_inproc_channel(0)
    Condition
      Error:
      ! mizu: capacity must be a positive integer

---

    Code
      mizu_inproc_channel(1.5)
    Condition
      Error:
      ! mizu: capacity must be a positive integer

---

    Code
      mizu_recv("x")
    Condition
      Error:
      ! mizu: ch must be a channel created by mizu_inproc_channel()

---

    Code
      mizu_recv(mizu_inproc_channel(1), timeout = -1)
    Condition
      Error:
      ! mizu: timeout must be a non-negative number

