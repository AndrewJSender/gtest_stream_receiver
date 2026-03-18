# GoogleTest Streaming Receiver (Python)

This server accepts GoogleTest streaming TCP lines.

Each TCP connection is treated as one independent test session.

## Disclaimer

This project was built with AI assistance.

> "Built with curiosity, tested with care. — GitHub Copilot"

## 1) Run the server

```bash
python3 gtest_stream_server.py --host 0.0.0.0 --port 8080 --out-dir ./data
```

## 2) Configure your test binary

Pass the gtest streaming flag to stream to this server:

```bash
./your_gtest_binary --gtest_stream_result_to=127.0.0.1:8080
```

If your test process runs on another machine, use the server machine IP.

## 3) Output files

After stream data arrives, the server writes one folder per connection under `--out-dir`:

- `data/session_000001_YYYYMMDD_HHMMSS_<client>/events.jsonl`: one JSON object per stream line/event
- `data/session_000001_YYYYMMDD_HHMMSS_<client>/summary.json`: final summary generated when that connection disconnects
- `data/session_000001_YYYYMMDD_HHMMSS_<client>/output.txt`: GoogleTest-style text report appended in real time as events arrive

The final `summary.json` includes:

- `passed_tests`: list of passed test names and `time_used_seconds`
- `failed_tests`: list of failed test names and `time_used_seconds`
- event counters and connection start/end timestamps

## 4) Quick smoke test

```bash
printf 'gtest_streaming_protocol_version=1.0\nevent=TestStart&name=Demo&status=RUN\n' | nc 127.0.0.1 8080
```

Then inspect the generated session folder inside `data/`.
