import json

import httplib2
import pytest
from googleapiclient.errors import HttpError

import inbox_stats as ix


# ------------------------------------------------------------------ fakes
class FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.sleeps = []

    def now(self):
        return self.t

    def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def http_error(status, reason=None, retry_after=None):
    headers = {"status": str(status)}
    if retry_after is not None:
        headers["retry-after"] = str(retry_after)
    body = {"error": {"errors": [{"reason": reason}] if reason else []}}
    return HttpError(httplib2.Response(headers), json.dumps(body).encode())


def make_msg(mid, thread=None, labels=("INBOX",), d=1_700_000_000_000, size=1000,
             sender="Bob <bob@x.com>"):
    return {"id": mid, "threadId": thread or mid, "labelIds": list(labels),
            "internalDate": str(d), "sizeEstimate": size,
            "payload": {"headers": [{"name": "From", "value": sender}]}}


class FakeRequest:
    def __init__(self, kind, kwargs, svc):
        self.kind, self.kwargs, self.svc = kind, kwargs, svc

    def execute(self):
        return self.svc.run_simple(self.kind, self.kwargs)


class FakeBatch:
    def __init__(self, svc, callback):
        self.svc, self.callback, self.items = svc, callback, []

    def add(self, request, callback=None, request_id=None):
        self.items.append((request, request_id))

    def execute(self):
        self.svc.on_execute(self)
        for req, rid in self.items:
            outcome = self.svc.outcome(rid)
            if isinstance(outcome, Exception):
                self.callback(rid, None, outcome)
            else:
                self.callback(rid, outcome, None)


class FakeService:
    """outcome_fn(mid, attempt) -> Exception or None (None = success)."""

    def __init__(self, messages, outcome_fn=None, clock=None, page=3):
        self.msgs = {m["id"]: m for m in messages}
        self.outcome_fn = outcome_fn or (lambda mid, attempt: None)
        self.clock = clock
        self.page = page
        self.attempts = {}
        self.fetched = []          # every message id requested, in order
        self.requests = []         # kwargs of every messages.get
        self.sends = []            # (time, units) per batch execute
        self.batches = 0
        self.interrupt_on_batch = None

    def users(self):
        return self

    def messages_(self):
        return self

    # users().messages().get/list
    def messages(self):
        svc = self

        class M:
            def get(_, **kw):
                svc.requests.append(kw)
                return FakeRequest("get", kw, svc)

            def list(_, **kw):
                return FakeRequest("list", kw, svc)
        return M()

    def new_batch_http_request(self, callback=None):
        return FakeBatch(self, callback)

    def on_execute(self, batch):
        self.batches += 1
        if self.interrupt_on_batch == self.batches:
            raise KeyboardInterrupt
        if self.clock:
            self.sends.append((self.clock.now(), len(batch.items) * ix.COST_GET))

    def outcome(self, mid):
        n = self.attempts[mid] = self.attempts.get(mid, 0) + 1
        err = self.outcome_fn(mid, n)
        if err is not None:
            return err
        self.fetched.append(mid)
        return self.msgs[mid]

    def run_simple(self, kind, kw):
        if kind == "list":
            ids = list(self.msgs)
            start = int(kw.get("pageToken") or 0)
            chunk = ids[start:start + self.page]
            resp = {"messages": [{"id": i} for i in chunk]}
            if start + self.page < len(ids):
                resp["nextPageToken"] = str(start + self.page)
            return resp
        raise AssertionError(kind)


def fetcher_for(svc, clock, rate=200, batch_size=50):
    limiter = ix.TokenBucket(rate, clock=clock.now, sleep=clock.sleep)
    return ix.Fetcher(svc, limiter, sleep=clock.sleep, rand=lambda: 0.0,
                      batch_size=batch_size), limiter


# ----------------------------------------------------------- token bucket
def test_token_bucket_never_exceeds_budget():
    clock = FakeClock()
    limiter = ix.TokenBucket(200, clock=clock.now, sleep=clock.sleep)
    t0, sent = clock.now(), []
    for cost in [250, 250, 5, 250, 125, 250, 250, 5, 250]:
        limiter.acquire(cost)
        sent.append((clock.now(), cost))
        clock.t += 0.01  # simulated request time
    total_before = 0
    for when, cost in sent:
        assert total_before <= 200 * (when - t0) + 1e-6
        total_before += cost
    # whole run: everything but the final send fits the elapsed-time budget
    assert total_before - sent[-1][1] <= 200 * (sent[-1][0] - t0) + 1e-6


def test_token_bucket_throttle_halves_and_recovers():
    clock = FakeClock()
    lim = ix.TokenBucket(200, clock=clock.now, sleep=clock.sleep)
    lim.on_throttle()
    assert lim.rate == 100
    lim.on_throttle()
    assert lim.rate == 50
    for _ in range(ix.CLEAN_BATCHES_TO_RECOVER - 1):
        lim.on_clean()
    assert lim.rate == 50
    lim.on_clean()
    assert lim.rate == 100
    for _ in range(ix.CLEAN_BATCHES_TO_RECOVER):
        lim.on_clean()
    assert lim.rate == 200
    for _ in range(ix.CLEAN_BATCHES_TO_RECOVER * 2):
        lim.on_clean()
    assert lim.rate == 200


# ------------------------------------------------------- reactive handling
def test_no_errors_still_respects_budget():
    clock = FakeClock()
    svc = FakeService([make_msg("m%d" % i) for i in range(400)], clock=clock)
    f, _ = fetcher_for(svc, clock, rate=200, batch_size=50)
    ids = list(svc.msgs)
    t0 = clock.now()
    for i in range(0, len(ids), 50):
        recs, lost = f.fetch_batch(ids[i:i + 50])
        assert lost == 0 and len(recs) == 50
    assert clock.sleeps  # limiter slept even though no error ever occurred
    before = 0
    for when, units in svc.sends:
        assert before <= 200 * (when - t0) + 1e-6
        before += units


def test_429_halves_budget_and_retries_only_failed_ids():
    clock = FakeClock()
    svc = FakeService([make_msg(c) for c in "abcd"],
                      lambda mid, n: http_error(429) if mid in "bc" and n == 1 else None,
                      clock=clock)
    f, lim = fetcher_for(svc, clock)
    recs, lost = f.fetch_batch(list("abcd"))
    assert lost == 0 and sorted(r["id"] for r in recs) == list("abcd")
    assert svc.attempts == {"a": 1, "b": 2, "c": 2, "d": 1}
    assert lim.rate == 100
    assert clock.sleeps[-1] == pytest.approx(ix.BACKOFF_START)  # jitter fixed at 0


def test_repeated_throttling_in_one_batch_halves_once_and_retries_in_small_groups():
    clock = FakeClock()
    ids = ["m%d" % i for i in range(12)]
    svc = FakeService([make_msg(i) for i in ids],
                      lambda mid, n: http_error(429) if n <= 3 else None, clock=clock)
    f, lim = fetcher_for(svc, clock)
    recs, lost = f.fetch_batch(ids)
    assert lost == 0 and len(recs) == 12
    assert lim.rate == 100  # halved once from 200, not once per round
    assert max(units for _, units in svc.sends[1:]) <= ix.COST_GET * ix.RETRY_CHUNK


def test_403_rate_limit_reasons_are_throttling():
    for reason in ("rateLimitExceeded", "userRateLimitExceeded"):
        assert ix.classify(http_error(403, reason)) == "throttle"
    assert ix.classify(http_error(403, "insufficientPermissions")) == "fatal"


def test_retry_after_header_is_honored():
    clock = FakeClock()
    svc = FakeService([make_msg("a")],
                      lambda mid, n: http_error(429, retry_after=17) if n == 1 else None,
                      clock=clock)
    f, _ = fetcher_for(svc, clock)
    f.fetch_batch(["a"])
    assert 17.0 in clock.sleeps


def test_503_backoff_doubles_and_caps_with_jitter():
    clock = FakeClock()
    svc = FakeService([make_msg("a")],
                      lambda mid, n: http_error(503) if n <= 6 else None, clock=clock)
    f, lim = fetcher_for(svc, clock)
    f.rand = lambda: 1.0  # maximum jitter
    recs, lost = f.fetch_batch(["a"])
    assert lost == 0 and len(recs) == 1
    waits = [s for s in clock.sleeps if s >= 1]
    assert waits[:6] == pytest.approx([6.25, 12.5, 25, 50, 75, 75])  # cap 60 * 1.25
    assert lim.rate == 200  # 5xx does not halve the budget


def test_abandons_after_max_rounds_and_continues():
    clock = FakeClock()
    svc = FakeService([make_msg("a"), make_msg("b")],
                      lambda mid, n: http_error(503) if mid == "a" else None, clock=clock)
    f, _ = fetcher_for(svc, clock)
    recs, lost = f.fetch_batch(["a", "b"])
    assert lost == 1 and [r["id"] for r in recs] == ["b"]
    assert svc.attempts["a"] == ix.MAX_RETRY_ROUNDS + 1


def test_404_skipped_and_other_errors_fatal():
    clock = FakeClock()
    svc = FakeService([make_msg("a"), make_msg("b")],
                      lambda mid, n: http_error(404) if mid == "a" else None, clock=clock)
    f, _ = fetcher_for(svc, clock)
    recs, lost = f.fetch_batch(["a", "b"])
    assert lost == 0 and [r["id"] for r in recs] == ["b"] and svc.attempts["a"] == 1

    svc = FakeService([make_msg("a")], lambda mid, n: http_error(400), clock=clock)
    f, _ = fetcher_for(svc, clock)
    with pytest.raises(ix.FatalAPIError):
        f.fetch_batch(["a"])


def test_requests_never_ask_for_body_subject_or_snippet():
    clock = FakeClock()
    svc = FakeService([make_msg("a")], clock=clock)
    f, _ = fetcher_for(svc, clock)
    f.fetch_batch(["a"])
    kw = svc.requests[0]
    assert kw["format"] == "metadata" and kw["metadataHeaders"] == ["From"]
    assert kw["fields"] == ix.MESSAGE_FIELDS
    for banned in ("snippet", "body", "parts", "Subject", "raw"):
        assert banned not in kw["fields"]
    assert ix.SCOPES == ["https://www.googleapis.com/auth/gmail.readonly"]


# ------------------------------------------------------------- cache resume
def test_cache_resume_fetches_only_missing(tmp_path):
    cache = str(tmp_path / "c.jsonl")
    clock = FakeClock()
    msgs = [make_msg("m%d" % i) for i in range(7)]
    svc = FakeService(msgs, clock=clock)
    svc.interrupt_on_batch = 3
    lim = ix.TokenBucket(200, clock=clock.now, sleep=clock.sleep)
    recs, interrupted, _ = ix.sync_cache(svc, cache, lim, batch_size=2, sleep=clock.sleep)
    assert interrupted and len(ix.load_cache(cache)) == 4

    svc2 = FakeService(msgs, clock=clock)
    recs, interrupted, abandoned = ix.sync_cache(svc2, cache, lim, batch_size=2,
                                                 sleep=clock.sleep)
    assert not interrupted and abandoned == 0
    assert sorted(svc2.fetched) == ["m4", "m5", "m6"]
    assert len(recs) == 7


def test_cache_ignores_ids_gone_from_mailbox_and_tolerates_partial_line(tmp_path):
    cache = tmp_path / "c.jsonl"
    cache.write_text(json.dumps(ix.reduce_message(make_msg("gone"))) + "\n{\"id\": \"tr")
    clock = FakeClock()
    svc = FakeService([make_msg("a")], clock=clock)
    lim = ix.TokenBucket(200, clock=clock.now, sleep=clock.sleep)
    recs, _, _ = ix.sync_cache(svc, str(cache), lim, sleep=clock.sleep)
    assert list(recs) == ["a"]


def test_refresh_labels_refetches_unread_and_inbox_only(tmp_path):
    cache = str(tmp_path / "c.jsonl")
    clock = FakeClock()
    msgs = [make_msg("a", labels=["INBOX", "UNREAD"]), make_msg("b", labels=["CATEGORY_UPDATES"])]
    svc = FakeService(msgs, clock=clock)
    lim = ix.TokenBucket(200, clock=clock.now, sleep=clock.sleep)
    ix.sync_cache(svc, cache, lim, sleep=clock.sleep)
    svc2 = FakeService(msgs, clock=clock)
    ix.sync_cache(svc2, cache, lim, sleep=clock.sleep, refresh_labels=True)
    assert svc2.fetched == ["a"]


def test_parse_sender_and_reduce():
    assert ix.parse_sender('"Bob B" <Bob@X.com>') == "bob@x.com"
    assert ix.parse_sender("") == "(unknown)"
    assert ix.parse_sender(None) == "(unknown)"
    r = ix.reduce_message(make_msg("a", thread="t", size=5, d=42))
    assert r == {"id": "a", "t": "t", "l": ["INBOX"], "d": 42, "s": 5, "f": "bob@x.com"}


# -------------------------------------------------------------- aggregation
def rec(mid, t, f, labels=(), d=1000, s=10):
    return {"id": mid, "t": t, "l": list(labels), "d": d, "s": s, "f": f}


def test_thread_aggregation():
    recs = [
        rec("1", "T", "late@x.com", ["INBOX"], d=3000, s=5),
        rec("2", "T", "first@x.com", ["UNREAD", "CATEGORY_UPDATES"], d=1000, s=50),
        rec("3", "T", "late@x.com", ["SENT"], d=2000, s=7),
    ]
    (t,) = ix.build_threads(recs, "me@x.com")
    assert t["sender"] == "first@x.com"
    assert t["labels"] == {"INBOX", "UNREAD", "CATEGORY_UPDATES", "SENT"}
    assert t["newest"] == 3000 and t["size"] == 62 and t["max_size"] == 50
    assert t["unread"] and t["category"] == "Updates"


def test_owner_threads_excluded_and_category_order():
    recs = [rec("1", "A", "me@x.com"), rec("2", "B", "o@x.com",
                                          ["CATEGORY_SOCIAL", "CATEGORY_PROMOTIONS"])]
    threads = ix.build_threads(recs, "me@x.com")
    assert [t["sender"] for t in threads] == ["o@x.com"]
    assert threads[0]["category"] == "Promotions"
    assert ix.thread_category(set()) == "Primary/Other"


# ------------------------------------------------------------- category mix
def test_category_mix_rule():
    assert ix.category_mix(["Promotions"] * 8 + ["Updates"] * 2) == "Promotions"
    assert ix.category_mix(["Promotions"] * 6 + ["Updates"] * 4) == "Promotions 60%, Updates 40%"
    assert ix.category_mix(["Social"]) == "Social"
    assert ix.category_mix(["Social"] * 79 + ["Updates"] * 21) == "Social 79%, Updates 21%"


# ----------------------------------------------------------------- segments
NOW = 2_000_000_000_000


def thread(labels=(), newest=NOW, max_size=10, unread=None):
    labels = set(labels)
    return {"labels": labels, "newest": newest, "max_size": max_size, "size": max_size,
            "unread": "UNREAD" in labels if unread is None else unread}


def test_segment_rules():
    segs = ix.make_segments(["Label_1"], NOW)
    old1, old2 = NOW - 366 * ix.DAY_MS, NOW - 731 * ix.DAY_MS

    assert segs["Inbox"](thread(["INBOX"])) and not segs["Inbox"](thread(["SENT"]))
    for name, label in [("Promotions", "CATEGORY_PROMOTIONS"), ("Social", "CATEGORY_SOCIAL"),
                        ("Updates", "CATEGORY_UPDATES"), ("Forums", "CATEGORY_FORUMS")]:
        assert segs[name](thread([label])) and not segs[name](thread(["INBOX"]))

    assert segs["Unread over 1yr"](thread(["UNREAD"], newest=old1))
    assert not segs["Unread over 1yr"](thread(["UNREAD"], newest=NOW - 364 * ix.DAY_MS))
    assert not segs["Unread over 1yr"](thread([], newest=old1))

    assert segs["Inbox over 2yr"](thread(["INBOX"], newest=old2))
    assert not segs["Inbox over 2yr"](thread(["INBOX"], newest=old1))
    assert not segs["Inbox over 2yr"](thread([], newest=old2))

    assert segs["Large over 10MB"](thread(max_size=10 * 1024 * 1024))
    assert not segs["Large over 10MB"](thread(max_size=10 * 1024 * 1024 - 1))

    assert segs["Archived unlabeled"](thread(["CATEGORY_UPDATES", "UNREAD"]))
    assert segs["Archived unlabeled"](thread([]))
    for lab in ("INBOX", "SENT", "DRAFT", "Label_1"):
        assert not segs["Archived unlabeled"](thread([lab]))

    assert segs["All mail"](thread())


# ------------------------------------------------------------------- report
def test_report_writes_tabs_and_keeps_formula_like_senders_as_text(tmp_path):
    import openpyxl
    threads = ix.build_threads([
        rec("1", "A", "=1+1@x.com", ["INBOX", "UNREAD"], d=NOW - ix.DAY_MS, s=2 * ix.MIB),
        rec("2", "B", "a@x.com", ["CATEGORY_PROMOTIONS"], d=NOW - ix.DAY_MS),
    ], "me@x.com")
    out = tmp_path / "o.xlsx"
    ix.write_report(str(out), threads, [], NOW)
    wb = openpyxl.load_workbook(out)
    assert wb.sheetnames[0] == "Summary" and "Archived unlabeled" in wb.sheetnames
    ws = wb["All mail"]
    assert ws.freeze_panes == "A2"
    cells = {ws.cell(r, 1).value: ws.cell(r, 1).data_type for r in (2, 3)}
    assert cells["=1+1@x.com"] == "s"
    assert [c.value for c in wb["Summary"][1]] == ["Segment", "Threads", "Unread threads", "Size MB"]
