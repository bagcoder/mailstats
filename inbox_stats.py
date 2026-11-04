#!/usr/bin/env python3
"""inbox_stats.py - read-only Gmail mailbox statistics -> Excel report.

Reads ONLY metadata (sender address, labels, date, size, thread id).
Never fetches bodies, subjects, snippets or attachments. Talks only to Google.
Scope used: gmail.readonly. No method that modifies, deletes or sends is called.
"""
import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from email.utils import parseaddr

import pandas as pd
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ---------------------------------------------------------------- constants
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
CREDENTIALS_FILE = "credentials.json"
TOKEN_FILE = "token.json"
CACHE_FILE = "messages_cache.jsonl"
OUTPUT_FILE = "inbox_stats.xlsx"

# Gmail's per-user limit is 6,000 quota units/minute (100/s), as shown under
# Quotas & System Limits in the Cloud Console (older docs said 250/s; the console
# lists that as "Previous quota"). See https://developers.google.com/gmail/api/reference/quota
# Default budget leaves ~25% headroom. messages.get = 5 units, messages.list = 5 units/page.
UNITS_PER_SECOND = 75
MIN_UNITS_PER_SECOND = 25
COST_GET = 5
COST_LIST = 5
BATCH_SIZE = 10
RETRY_CHUNK = 5  # retried messages are re-sent in small groups to avoid bursts
LIST_PAGE_SIZE = 500

BACKOFF_START = 5.0
BACKOFF_CAP = 60.0
JITTER = 0.25
MAX_RETRY_ROUNDS = 8
CLEAN_BATCHES_TO_RECOVER = 5

TOP_N = 100
DAY_MS = 86_400_000
MIB = 1024 * 1024
LARGE_BYTES = 10 * MIB
UNKNOWN_SENDER = "(unknown)"

THROTTLE_REASONS = {"rateLimitExceeded", "userRateLimitExceeded"}
SERVER_STATUSES = {500, 502, 503, 504}
MESSAGE_FIELDS = "id,threadId,labelIds,internalDate,sizeEstimate,payload/headers"

CATEGORIES = [
    ("CATEGORY_PROMOTIONS", "Promotions"),
    ("CATEGORY_SOCIAL", "Social"),
    ("CATEGORY_UPDATES", "Updates"),
    ("CATEGORY_FORUMS", "Forums"),
]
OTHER_CATEGORY = "Primary/Other"
CATEGORY_DOMINANCE = 0.80

COLUMNS = ["Sender", "Threads", "Unread", "Unread %", "Newest", "Size MB", "Category"]


class FatalAPIError(Exception):
    """Non-retryable API error; the cache is kept."""


class RetryableError(Exception):
    """Transient failure that is not an HttpError (network, missing response)."""


# ------------------------------------------------------------ rate limiting
class TokenBucket:
    """Proactive limiter in Gmail quota units.

    A batch is charged before it is sent: the next send is not allowed until
    cost/rate seconds after the previous send, so cumulative units sent never
    exceed rate * elapsed. Clock and sleep are injectable for tests.
    """

    def __init__(self, rate=UNITS_PER_SECOND, clock=time.monotonic, sleep=time.sleep):
        self.default_rate = float(rate)
        self.rate = float(rate)
        self.clock = clock
        self.sleep = sleep
        self.next_free = None
        self.clean_streak = 0

    def acquire(self, cost):
        now = self.clock()
        if self.next_free is not None and now < self.next_free:
            self.sleep(self.next_free - now)
            now = self.clock()
        self.next_free = now + cost / self.rate

    def on_throttle(self):
        self.rate = max(MIN_UNITS_PER_SECOND, self.rate / 2)
        self.clean_streak = 0

    def on_clean(self):
        self.clean_streak += 1
        if self.clean_streak >= CLEAN_BATCHES_TO_RECOVER and self.rate < self.default_rate:
            self.rate = min(self.default_rate, self.rate * 2)
            self.clean_streak = 0


# ------------------------------------------------------------ error helpers
def error_reason(exc):
    try:
        data = json.loads(exc.content.decode("utf-8", "replace"))
        err = data.get("error", {})
        errors = err.get("errors") or err.get("details") or []
        for item in errors:
            if isinstance(item, dict) and item.get("reason"):
                return item["reason"]
    except (ValueError, AttributeError, TypeError):
        pass
    return None


def describe_error(exc):
    """Short 'status reason: message' text for diagnostics (no message content involved)."""
    if not isinstance(exc, HttpError):
        return type(exc).__name__
    text = "%s %s" % (exc.resp.status, error_reason(exc) or "")
    try:
        msg = json.loads(exc.content.decode("utf-8", "replace"))["error"]["message"]
        text += ": " + msg
    except (ValueError, KeyError, AttributeError, TypeError):
        pass
    return text.strip()


def retry_after_seconds(exc):
    try:
        return float(exc.resp.get("retry-after"))
    except (TypeError, ValueError, AttributeError):
        return None


def classify(exc):
    """Return 'notfound', 'throttle', 'server' or 'fatal'."""
    if not isinstance(exc, HttpError):
        return "server"
    status = exc.resp.status
    if status == 404:
        return "notfound"
    if status == 429:
        return "throttle"
    if status == 403 and error_reason(exc) in THROTTLE_REASONS:
        return "throttle"
    if status in SERVER_STATUSES:
        return "server"
    return "fatal"


# ------------------------------------------------------------------ parsing
def parse_sender(from_header):
    addr = parseaddr(from_header or "")[1].strip().lower()
    return addr or UNKNOWN_SENDER


def reduce_message(msg):
    headers = (msg.get("payload") or {}).get("headers") or []
    from_value = next((h.get("value") for h in headers if h.get("name", "").lower() == "from"), "")
    return {
        "id": msg["id"],
        "t": msg.get("threadId"),
        "l": msg.get("labelIds") or [],
        "d": int(msg.get("internalDate", 0)),
        "s": int(msg.get("sizeEstimate", 0)),
        "f": parse_sender(from_value),
    }


# -------------------------------------------------------------------- cache
def load_cache(path):
    records = {}
    if not os.path.exists(path):
        return records
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                rec = json.loads(line)
                records[rec["id"]] = rec
            except (ValueError, KeyError):
                continue  # partial line from an interrupted run
    return records


def open_private_append(path):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    return os.fdopen(fd, "a", encoding="utf-8")


def append_records(fh, records):
    for rec in records:
        fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
    fh.flush()


def rewrite_cache(path, records):
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
    os.replace(tmp, path)


# -------------------------------------------------------------------- fetch
def list_all_ids(service, limiter, notify=None):
    ids, token = [], None
    while True:
        limiter.acquire(COST_LIST)
        resp = service.users().messages().list(
            userId="me", maxResults=LIST_PAGE_SIZE, includeSpamTrash=False, pageToken=token
        ).execute()
        ids.extend(m["id"] for m in resp.get("messages", []))
        if notify:
            notify("Listing messages: %d found so far" % len(ids))
        token = resp.get("nextPageToken")
        if not token:
            return ids


class Fetcher:
    def __init__(self, service, limiter, sleep=time.sleep, rand=random.random,
                 batch_size=BATCH_SIZE, notify=None):
        self.notify = notify
        self.service = service
        self.limiter = limiter
        self.sleep = sleep
        self.rand = rand
        self.batch_size = batch_size

    def _send(self, ids):
        results, errors = {}, {}

        def callback(request_id, response, exception):
            if exception is None:
                results[request_id] = response
            else:
                errors[request_id] = exception

        batch = self.service.new_batch_http_request(callback=callback)
        for mid in ids:
            batch.add(
                self.service.users().messages().get(
                    userId="me", id=mid, format="metadata",
                    metadataHeaders=["From"], fields=MESSAGE_FIELDS),
                request_id=mid,
            )
        try:
            batch.execute()
        except (HttpError, OSError) as exc:
            for mid in ids:
                if mid not in results and mid not in errors:
                    errors[mid] = exc
        for mid in ids:
            if mid not in results and mid not in errors:
                errors[mid] = RetryableError("no response for " + mid)
        return results, errors

    def _send_chunked(self, ids, rounds):
        """First attempt sends the whole batch; retries go out in small groups."""
        if rounds == 0:
            return self._send(ids)
        results, errors = {}, {}
        for i in range(0, len(ids), RETRY_CHUNK):
            chunk = ids[i:i + RETRY_CHUNK]
            if i:
                self.limiter.acquire(COST_GET * len(chunk))
            r, e = self._send(chunk)
            results.update(r)
            errors.update(e)
        return results, errors

    def _delay(self, round_no, retry_after):
        if retry_after is not None:
            return retry_after
        base = min(BACKOFF_START * (2 ** round_no), BACKOFF_CAP)
        return base * (1 + JITTER * self.rand())

    def fetch_batch(self, ids):
        """Fetch one batch with retries. Returns (records, abandoned_count)."""
        records, pending, rounds, halved = [], list(ids), 0, False
        while pending:
            self.limiter.acquire(COST_GET * min(len(pending), RETRY_CHUNK if rounds else len(pending)))
            results, errors = self._send_chunked(pending, rounds)
            records.extend(reduce_message(m) for m in results.values())

            retry, throttled, retry_after, sample = [], False, None, None
            for mid, exc in errors.items():
                kind = classify(exc)
                if kind == "notfound":
                    continue  # deleted since listing
                if kind == "fatal":
                    raise FatalAPIError("Gmail API error on message %s: %s" % (mid, exc))
                retry.append(mid)
                if kind == "throttle":
                    throttled = True
                    sample = sample or exc
                ra = retry_after_seconds(exc)
                if ra is not None:
                    retry_after = max(retry_after or 0, ra)

            if throttled and not halved:
                halved = True  # one halving per batch, however many retry rounds it takes
                self.limiter.on_throttle()
                if self.notify:
                    self.notify("Throttled by Gmail (%s); budget now %.0f units/s"
                                % (describe_error(sample), self.limiter.rate))
            elif not errors and rounds == 0:
                self.limiter.on_clean()
            if not retry:
                return records, 0
            if rounds >= MAX_RETRY_ROUNDS:
                return records, len(retry)
            wait = self._delay(rounds, retry_after)
            if self.notify:
                self.notify("Retrying %d message(s) in %.0fs (round %d/%d)"
                            % (len(retry), wait, rounds + 1, MAX_RETRY_ROUNDS))
            self.sleep(wait)
            rounds += 1
            pending = retry
        return records, 0


def sync_cache(service, cache_path, limiter, batch_size=BATCH_SIZE, sleep=time.sleep,
               refresh_labels=False, progress=None, rand=random.random, notify=None):
    """Bring the cache up to date. Returns (records_in_mailbox, interrupted, abandoned)."""
    cached = load_cache(cache_path)
    if refresh_labels:
        # UNREAD/INBOX are the labels that change; re-fetch only those messages.
        cached = {k: v for k, v in cached.items() if not {"UNREAD", "INBOX"} & set(v["l"])}
        rewrite_cache(cache_path, cached.values())
    try:
        ids = list_all_ids(service, limiter, notify)
    except KeyboardInterrupt:
        return {}, True, 0
    in_mailbox = set(ids)
    missing = [i for i in ids if i not in cached]
    fetcher = Fetcher(service, limiter, sleep=sleep, batch_size=batch_size, rand=rand,
                      notify=notify)
    abandoned, done, started = 0, 0, time.monotonic()
    interrupted = False
    with open_private_append(cache_path) as fh:
        try:
            for start in range(0, len(missing), batch_size):
                chunk = missing[start:start + batch_size]
                recs, lost = fetcher.fetch_batch(chunk)
                append_records(fh, recs)
                for rec in recs:
                    cached[rec["id"]] = rec
                abandoned += lost
                done += len(chunk)
                if progress:
                    elapsed = time.monotonic() - started
                    eta = elapsed / done * (len(missing) - done) / 60
                    progress(done, len(missing), eta)
        except KeyboardInterrupt:
            interrupted = True
    return {i: cached[i] for i in ids if i in cached and i in in_mailbox}, interrupted, abandoned


# ----------------------------------------------------------------- analysis
def thread_category(labels):
    for label, name in CATEGORIES:
        if label in labels:
            return name
    return OTHER_CATEGORY


def build_threads(records, owner_email):
    groups = {}
    for rec in records:
        groups.setdefault(rec["t"] or rec["id"], []).append(rec)
    threads = []
    for tid, msgs in groups.items():
        earliest = min(msgs, key=lambda m: m["d"])
        if earliest["f"] == (owner_email or "").lower():
            continue
        labels = set()
        for m in msgs:
            labels.update(m["l"])
        threads.append({
            "id": tid,
            "sender": earliest["f"],
            "labels": labels,
            "newest": max(m["d"] for m in msgs),
            "size": sum(m["s"] for m in msgs),
            "max_size": max(m["s"] for m in msgs),
            "unread": "UNREAD" in labels,
            "category": thread_category(labels),
        })
    return threads


def category_mix(categories):
    """Name if one category holds >= 80%, else a 'Promotions 60%, Updates 40%' breakdown."""
    counts = Counter(categories)
    total = sum(counts.values())
    if not total:
        return OTHER_CATEGORY
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    if ranked[0][1] / total >= CATEGORY_DOMINANCE:
        return ranked[0][0]
    return ", ".join("%s %d%%" % (name, round(100 * n / total)) for name, n in ranked)


def sender_categories(threads):
    by_sender = {}
    for t in threads:
        by_sender.setdefault(t["sender"], []).append(t["category"])
    return {s: category_mix(c) for s, c in by_sender.items()}


def make_segments(user_label_ids, now_ms):
    def has_cat(label):
        return lambda t: label in t["labels"]

    segs = {
        "Inbox": lambda t: "INBOX" in t["labels"],
        "Promotions": has_cat("CATEGORY_PROMOTIONS"),
        "Social": has_cat("CATEGORY_SOCIAL"),
        "Updates": has_cat("CATEGORY_UPDATES"),
        "Forums": has_cat("CATEGORY_FORUMS"),
        "Unread over 1yr": lambda t: t["unread"] and t["newest"] < now_ms - 365 * DAY_MS,
        "Inbox over 2yr": lambda t: "INBOX" in t["labels"] and t["newest"] < now_ms - 730 * DAY_MS,
        "Large over 10MB": lambda t: t["max_size"] >= LARGE_BYTES,
        "Archived unlabeled": lambda t: not ({"INBOX", "SENT", "DRAFT"} & t["labels"])
        and not (t["labels"] & set(user_label_ids)),
        "All mail": lambda t: True,
    }
    return segs


def summarize(name, threads):
    return {
        "Segment": name,
        "Threads": len(threads),
        "Unread threads": sum(t["unread"] for t in threads),
        "Size MB": round(sum(t["size"] for t in threads) / MIB, 1),
    }


def sender_table(threads, categories, top_n=TOP_N):
    by_sender = {}
    for t in threads:
        by_sender.setdefault(t["sender"], []).append(t)
    rows = []
    for sender, ts in by_sender.items():
        unread = sum(t["unread"] for t in ts)
        newest = max(t["newest"] for t in ts)
        rows.append({
            "Sender": sender,
            "Threads": len(ts),
            "Unread": unread,
            "Unread %": round(100 * unread / len(ts), 1),
            "Newest": pd.Timestamp(newest, unit="ms").date(),
            "Size MB": round(sum(t["size"] for t in ts) / MIB, 1),
            "Category": categories.get(sender, OTHER_CATEGORY),
        })
    rows.sort(key=lambda r: (-r["Threads"], r["Sender"]))
    return pd.DataFrame(rows[:top_n], columns=COLUMNS)


# ------------------------------------------------------------------- report
def write_report(path, threads, user_label_ids, now_ms):
    categories = sender_categories(threads)
    segments = make_segments(user_label_ids, now_ms)
    summary, tables = [], {}
    for name, rule in segments.items():
        members = [t for t in threads if rule(t)]
        summary.append(summarize(name, members))
        tables[name] = sender_table(members, categories)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(summary, columns=["Segment", "Threads", "Unread threads", "Size MB"]).to_excel(
            writer, sheet_name="Summary", index=False)
        for name, df in tables.items():
            df.to_excel(writer, sheet_name=name, index=False)
        for ws in writer.book.worksheets:
            ws.freeze_panes = "A2"
            for col in ws.columns:
                width = max(len(str(c.value)) if c.value is not None else 0 for c in col)
                ws.column_dimensions[col[0].column_letter].width = min(width + 2, 60)
            if ws.title != "Summary":
                # Sender strings may start with '=': force text so Excel never evaluates them.
                for row in ws.iter_rows(min_row=2):
                    for cell in (row[0], row[6]):
                        if isinstance(cell.value, str):
                            cell.data_type = "s"


# --------------------------------------------------------------------- auth
def get_credentials(token_file=TOKEN_FILE, creds_file=CREDENTIALS_FILE):
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if creds and not creds.valid:
        try:
            if creds.refresh_token:
                creds.refresh(Request())
            else:
                creds = None
        except RefreshError:
            os.remove(token_file)
            creds = None
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(creds_file, SCOPES)
        creds = flow.run_local_server(port=0)
        fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as fh:
            fh.write(creds.to_json())
    return creds


def user_label_ids(service):
    labels = service.users().labels().list(userId="me").execute().get("labels", [])
    return [l["id"] for l in labels if l.get("type") == "user"]


# --------------------------------------------------------------------- main
def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Read-only Gmail statistics -> Excel.")
    p.add_argument("--fresh", action="store_true", help="delete the cache and refetch everything")
    p.add_argument("--refresh-labels", action="store_true",
                   help="refetch cached messages that were UNREAD or in INBOX (labels change)")
    p.add_argument("--units-per-second", type=float, default=UNITS_PER_SECOND)
    p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    return p.parse_args(argv)


def print_progress(done, total, eta_min):
    print("\rFetched %d/%d messages, ~%.1f min remaining   " % (done, total, eta_min),
          end="", file=sys.stderr, flush=True)


def print_notice(text):
    print("\n" + text, file=sys.stderr, flush=True)


def main(argv=None):
    args = parse_args(argv)
    if args.fresh and os.path.exists(CACHE_FILE):
        os.remove(CACHE_FILE)
    service = build("gmail", "v1", credentials=get_credentials(), cache_discovery=False)
    limiter = TokenBucket(args.units_per_second)
    owner = service.users().getProfile(userId="me").execute()["emailAddress"].lower()
    try:
        records, interrupted, abandoned = sync_cache(
            service, CACHE_FILE, limiter, batch_size=args.batch_size,
            refresh_labels=args.refresh_labels, progress=print_progress,
            notify=print_notice)
    except FatalAPIError as exc:
        print("\nStopping: %s\nCache kept in %s." % (exc, CACHE_FILE), file=sys.stderr)
        return 1
    print(file=sys.stderr)
    if interrupted:
        print("Interrupted. Cache kept; rerun to continue.", file=sys.stderr)
        return 130
    if abandoned:
        print("%d messages abandoned after repeated throttling; a later run retries them."
              % abandoned, file=sys.stderr)
    threads = build_threads(records.values(), owner)
    now_ms = int(time.time() * 1000)
    write_report(OUTPUT_FILE, threads, user_label_ids(service), now_ms)
    print("Wrote %s (%d threads)." % (OUTPUT_FILE, len(threads)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
