# inbox_stats

Read-only Gmail analysis -> `inbox_stats.xlsx`. Fetches only sender address, labels,
date, size and thread id. Never fetches bodies, subjects, snippets or attachments.
Scope: `https://www.googleapis.com/auth/gmail.readonly`. Talks only to Google.

## One-time Google Cloud setup
1. Create a project at https://console.cloud.google.com/.
2. Enable the **Gmail API** (APIs & Services > Library).
3. OAuth consent screen: user type **External**; add your own address under **Test users**.
4. Credentials > Create credentials > OAuth client ID > **Desktop app**. Download the JSON and save it as `credentials.json` in this folder.

## Install and run
```
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python inbox_stats.py
```
The first run opens a browser for login (an "unverified app" warning is expected for your own project) and saves `token.json`.

Options:
- `--fresh` delete the cache and refetch everything (most accurate).
- `--refresh-labels` refetch cached messages that were UNREAD or in INBOX, since those labels change. Cached messages are otherwise never re-read.
- `--units-per-second N` rate budget (default 75; Gmail's per-user limit is 6,000 units/minute = 100/s).
- `--batch-size N` messages per batch request (default 10).

Ctrl+C is safe: the cache is kept and the next run continues. Speed depends on batch latency; expect a few to 20 messages/sec at the defaults.

## Notes
- Apps in Testing status get refresh tokens that expire after 7 days; the tool detects this and reruns the browser login.
- `token.json` and `messages_cache.jsonl` are private (mode 0600) and git-ignored. The token grants read access to your mailbox: revoke it at https://myaccount.google.com/permissions when done.

## Caveats and known limitations
- **Slow on large mailboxes.** Gmail limits each user to 6,000 quota units per minute (see Quotas & System Limits in the Cloud Console), and the limit is hard (not adjustable). In practice that is about 5 messages per second for `messages.get`, even though the documented cost is 5 units per call, so the real cost per call appears to be higher. A 50,000-message mailbox takes roughly 2-3 hours for the first run. Later runs fetch only new mail.
- **Throttle messages are normal.** The tool probes the limit and backs off. Retries lose nothing, so frequent "Throttled by Gmail" lines do not mean something is broken. The default budget (75 units/s) is based on the documented cost and will still be throttled sometimes.
- **Labels in the cache can go stale.** Cached messages are never re-read, so mail you later read or archive keeps its old labels. Use `--refresh-labels` (re-reads cached UNREAD/INBOX messages) or `--fresh` before relying on unread counts.
- **Counts are by thread, by sender of the first message.** A thread is attributed to whoever started it, so replies from other people do not count toward their totals. Threads you started are excluded entirely.
- **Sender only, not display names.** Senders are identified by lowercased email address, so one organization using many addresses shows up as many senders. Messages with no parseable From header are grouped as `(unknown)`.
- **Spam and Trash are excluded.**
- **Category is approximate.** It comes from Gmail's own category labels, which Gmail can change over time.
- **Sizes are Gmail's `sizeEstimate`,** not exact byte counts.
- **Google Cloud setup is required.** You create your own project and OAuth client. The app stays in Testing mode, so Google shows an "unverified app" warning and the login expires every 7 days. This tool is not designed for publishing to other users.
- **Not tested against a live mailbox in the test suite.** The tests use a fake Gmail service; real-world throttling behavior was tuned by hand.
- **Python 3.10 works but is past Google's library support;** 3.11 or later is recommended.

## License
MIT. See `LICENSE`. Provided as is, with no warranty. You are responsible for your own Google Cloud project, credentials and mailbox data.

## Tests
```
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

## How it works

### Program flow
1. **Login.** Uses `token.json` if valid. If the token is expired it refreshes it. If the refresh fails (`RefreshError`, e.g. the 7-day Testing-mode expiry), it deletes the token and reruns the browser login.
2. **Profile and cache.** Reads your own address via `users.getProfile` (to exclude threads you started). `--fresh` deletes the cache first. `--refresh-labels` drops cached messages that were UNREAD or in INBOX so they get refetched.
3. **List IDs.** Calls `messages.list` (500 per page, Spam and Trash excluded, archived mail included) and follows `nextPageToken`. This runs on every start: it finds new mail and drops cached messages that were deleted.
4. **Fetch what is missing.** IDs not already in `messages_cache.jsonl` are fetched in batches. Each message is requested with `format=metadata`, only the `From` header, and a field mask of `id, threadId, labelIds, internalDate, sizeEstimate, payload/headers`. Each result is reduced to one line (`id, t, l, d, s, f`) and appended to the cache, which is flushed after every batch.
5. **Report.** Messages are grouped into threads, aggregated, split into segments and written to `inbox_stats.xlsx`. If you pressed Ctrl+C, no report is written; the cache is kept for the next run.

### Pacing and retries
- **Proactive limiter (always on).** A token bucket counts Gmail quota units (5 per message, 5 per list page). Each batch is charged before it is sent, and the tool sleeps so average usage never exceeds the budget (`--units-per-second`, default 75).
- **Burst size.** A batch sends all its requests at once, so a big batch can be throttled even when the average rate is fine. If you see constant throttling, lower `--batch-size` (default 10) or `--units-per-second`.
- **Reactive handling.** HTTP 429, and 403 with reason `rateLimitExceeded` or `userRateLimitExceeded`, count as throttling. HTTP 500/502/503/504 and network errors are retried too. Only the individual failed messages are retried. The wait honors `Retry-After` if present, otherwise it is 5s doubling to a 60s cap, plus up to 25% jitter.
- **Budget adjustment.** A throttled batch halves the budget once (floor 25 units/s), however many retry rounds it needs, and retried messages are re-sent in groups of 5. After 5 clean batches in a row (no retries) the budget doubles back toward the default.
- **Giving up.** After 8 retry rounds, remaining messages are abandoned and counted; a later run retries them because they never reached the cache. A 404 means the message was deleted and is skipped. Any other error stops the run with a message and keeps the cache.
- **Output while running.** A progress line (fetched count, estimated minutes left), plus notices when listing, when throttled, and before a retry wait.

### Analysis
- **Threads.** Messages are grouped by thread ID. A thread's sender is the sender of its earliest message. Its labels are the union of all its messages' labels. Newest is the latest message time, size is the sum of message sizes, and max size is the largest single message. Threads you started are excluded. A thread is unread if any message is UNREAD.
- **Category.** First match of Promotions, Social, Updates, Forums, otherwise `Primary/Other`. A sender's category covers all their threads: one category with at least 80% is shown by name, otherwise a mix such as `Promotions 60%, Updates 40%`.
- **Segments** (one tab each): Inbox; Promotions, Social, Updates, Forums; Unread over 1yr (unread, newest message older than 365 days); Inbox over 2yr (newest older than 730 days); Large over 10MB (largest message at least 10 MiB); Archived unlabeled (no INBOX, SENT or DRAFT label and none of your own labels); All mail.
- **Output.** The Summary tab has Segment, Threads, Unread threads, Size MB for every sender in the segment. Each segment tab lists the top 100 senders by thread count with Threads, Unread, Unread %, Newest, Size MB and Category. Sender cells are stored as text so an address starting with `=` is never treated as a formula.

### Files
| File | Purpose |
|---|---|
| `credentials.json` | Your OAuth client (from Google Cloud) |
| `token.json` | Saved login (mode 0600) |
| `messages_cache.jsonl` | One metadata line per message, enables resume (mode 0600) |
| `inbox_stats.xlsx` | The report |
