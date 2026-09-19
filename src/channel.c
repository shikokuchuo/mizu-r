#include <R.h>
#include <Rinternals.h>

#include <stdlib.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <time.h>
#endif

/* The ring buffer: a preserved VECSXP mutated in place. The buffer never
   escapes to R, so in-place mutation breaks no copy-on-modify contract,
   and a VECSXP slot carries a NULL payload natively. */
typedef struct {
  SEXP buf;
  R_xlen_t cap;
  R_xlen_t head;
  R_xlen_t len;
  int closed;
} mizu_ring;

static mizu_ring *ring_get(SEXP ch) {
  if (TYPEOF(ch) != EXTPTRSXP) {
    error("mizu: invalid channel");
  }
  mizu_ring *r = (mizu_ring *) R_ExternalPtrAddr(ch);
  if (r == NULL) {
    error("mizu: invalid channel");
  }
  return r;
}

static void ring_free(SEXP ch) {
  mizu_ring *r = (mizu_ring *) R_ExternalPtrAddr(ch);
  if (r == NULL) {
    return;
  }
  R_ClearExternalPtr(ch);
  R_ReleaseObject(r->buf);
  free(r);
}

/* The sentinels are interned singletons: classed environments created
   in R and registered at load, preserved here for the process lifetime,
   so identity is a pointer test and a class look-alike payload can
   never forge one. */
static SEXP sent_full, sent_timeout, sent_closed;

SEXP mizu_sentinels_init(SEXP full, SEXP timeout, SEXP closed) {
  sent_full = full;
  R_PreserveObject(sent_full);
  sent_timeout = timeout;
  R_PreserveObject(sent_timeout);
  sent_closed = closed;
  R_PreserveObject(sent_closed);
  return R_NilValue;
}

/* A fresh environment carrying the same classes: a forwarded sentinel
   crosses as an ordinary copy, never as the singleton itself. */
static SEXP sentinel_dup(SEXP x) {
  SEXP cl = PROTECT(getAttrib(x, R_ClassSymbol));
  SEXP copy = PROTECT(R_NewEnv(R_EmptyEnv, FALSE, 0));
  classgets(copy, cl);
  UNPROTECT(2);
  return copy;
}

SEXP mizu_sentinel_check(SEXP x) {
  return ScalarLogical(
    x == sent_full || x == sent_timeout || x == sent_closed
  );
}

/* Monotonic clock in seconds: wall-clock adjustments must not stretch
   or shrink a timeout. */
static double mizu_now(void) {
#ifdef _WIN32
  LARGE_INTEGER freq, count;
  QueryPerformanceFrequency(&freq);
  QueryPerformanceCounter(&count);
  return (double) count.QuadPart / (double) freq.QuadPart;
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (double) ts.tv_sec + (double) ts.tv_nsec / 1e9;
#endif
}

/* Interruptible sleep; an early return (a signal) just means the wait
   loop re-checks the ring sooner. */
static void mizu_sleep(double seconds) {
#ifdef _WIN32
  DWORD ms = (DWORD) (seconds * 1e3);
  Sleep(ms > 0 ? ms : 1);
#else
  struct timespec ts;
  ts.tv_sec = (time_t) seconds;
  ts.tv_nsec = (long) ((seconds - (double) ts.tv_sec) * 1e9);
  nanosleep(&ts, NULL);
#endif
}

SEXP mizu_inproc_ring_create(SEXP capacity) {
  const R_xlen_t cap = asInteger(capacity);
  if (cap < 1) {
    error("mizu: capacity must be a positive integer");
  }
  SEXP buf = PROTECT(allocVector(VECSXP, cap));
  mizu_ring *r = (mizu_ring *) malloc(sizeof(mizu_ring));
  if (r == NULL) {
    error("mizu: failed to allocate channel");
  }
  R_PreserveObject(buf);
  UNPROTECT(1);
  r->buf = buf;
  r->cap = cap;
  r->head = 0;
  r->len = 0;
  r->closed = 0;
  SEXP ch = PROTECT(R_MakeExternalPtr(r, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ch, ring_free, TRUE);
  UNPROTECT(1);
  return ch;
}

/* Returns TRUE, or the mizu_full / mizu_closed sentinel. */
SEXP mizu_inproc_ring_send(SEXP ch, SEXP x) {
  mizu_ring *r = ring_get(ch);
  if (r->closed) {
    return sent_closed;
  }
  if (r->len == r->cap) {
    return sent_full;
  }
  SET_VECTOR_ELT(r->buf, (r->head + r->len) % r->cap, x);
  r->len++;
  return ScalarLogical(1);
}

/* Receive with a bounded wait: a pure spin while less than a millisecond
   remains (a sleep syscall would overshoot the budget), then a doubling
   sleep backoff capped at 100 ms. Returns the payload, or the
   mizu_timeout / mizu_closed sentinel. A payload that is
   itself a sentinel singleton crosses as an ordinary copy, so a returned
   singleton always tags a terminal state of the channel. */
SEXP mizu_inproc_ring_recv(SEXP ch, SEXP timeout) {
  mizu_ring *r = ring_get(ch);
  const double deadline = mizu_now() + asReal(timeout);
  double interval = 1e-6;
  for (;;) {
    R_CheckUserInterrupt();
    if (r->len > 0) {
      SEXP x = VECTOR_ELT(r->buf, r->head);
      SET_VECTOR_ELT(r->buf, r->head, R_NilValue);
      r->head = (r->head + 1) % r->cap;
      r->len--;
      if (x == sent_full || x == sent_timeout || x == sent_closed) {
        PROTECT(x);
        SEXP copy = sentinel_dup(x);
        UNPROTECT(1);
        return copy;
      }
      return x;
    }
    if (r->closed) {
      return sent_closed;
    }
    const double remaining = deadline - mizu_now();
    if (remaining <= 0) {
      return sent_timeout;
    }
    if (remaining > 1e-3) {
      mizu_sleep(interval < remaining ? interval : remaining);
      if (interval < 0.1) {
        interval *= 2;
      }
    }
  }
}

SEXP mizu_inproc_ring_close(SEXP ch) {
  ring_get(ch)->closed = 1;
  return ScalarLogical(1);
}

SEXP mizu_inproc_ring_stat(SEXP ch) {
  mizu_ring *r = ring_get(ch);
  SEXP st = PROTECT(allocVector(INTSXP, 3));
  INTEGER(st)[0] = (int) r->len;
  INTEGER(st)[1] = (int) r->cap;
  INTEGER(st)[2] = r->closed;
  UNPROTECT(1);
  return st;
}

static const R_CallMethodDef mizu_inproc_methods[] = {
  {"mizu_inproc_ring_create", (DL_FUNC) &mizu_inproc_ring_create, 1},
  {"mizu_inproc_ring_send", (DL_FUNC) &mizu_inproc_ring_send, 2},
  {"mizu_inproc_ring_recv", (DL_FUNC) &mizu_inproc_ring_recv, 2},
  {"mizu_inproc_ring_close", (DL_FUNC) &mizu_inproc_ring_close, 1},
  {"mizu_inproc_ring_stat", (DL_FUNC) &mizu_inproc_ring_stat, 1},
  {"mizu_sentinels_init", (DL_FUNC) &mizu_sentinels_init, 3},
  {"mizu_sentinel_check", (DL_FUNC) &mizu_sentinel_check, 1},
  {NULL, NULL, 0}
};

void R_init_mizu(DllInfo *dll) {
  R_registerRoutines(dll, NULL, mizu_inproc_methods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
}
