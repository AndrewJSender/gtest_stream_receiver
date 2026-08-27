# GoogleTest Streaming Receiver

This project accepts raw GoogleTest streaming TCP events, stores the raw event data, and can rebuild a structured JSON/XML model from that stream.

## Overview

There are two main pieces:

- `gtest_stream_server.py` listens for stream events from a GoogleTest binary over TCP.
- `process_events.py` reads the saved raw events and reconstructs nested test/session data.

The server does not emit a final per-test summary by itself. It stores raw events and leaves grouping/aggregation to the processor.

## Disclaimer

This project was built with AI assistance.

> "Built with curiosity, tested with care. — GitHub Copilot"

## 1) Run the receiver

```bash
python3 gtest_stream_server.py \
  --host 0.0.0.0 \
  --port 8080 \
  --out-dir ./data \
  --jsonl \
  --sqlite-db
```

### Server arguments

- `--host`: bind host, default `0.0.0.0`
- `--port`: TCP port, default `8080`
- `--out-dir`: directory for saved event files, default `./data`
- `--jsonl`: write raw events as JSON Lines to `events.jsonl`
- `--sqlite-db`: write raw events to SQLite database `events.db`
- `--log_level`: logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`)

At least one storage format must be selected. If both are set, both files are created in the same output directory.

## 2) Configure your test binary

Stream results to the receiver over TCP. The common GoogleTest command-line form is:

```bash
./your_gtest_binary --gtest_stream_result_to=127.0.0.1:8080
```

GoogleTest may also be driven via the environment variable form:

```bash
env GTEST_OUTPUT="stream_result_to=127.0.0.1:8080" ./your_gtest_binary
```

If the test runs on another machine, use that machine's IP address instead of `127.0.0.1`.

## 3) Raw event output

Each incoming TCP connection gets a new `session_id` counter. The server appends one record per line received.

Example event payload:

```json
{
  "utc": "2026-08-27T12:34:56.789012+00:00",
  "monotonic": 123.456,
  "session_id": 1,
  "client": "127.0.0.1:51234",
  "params": {
    "event": "teststart",
    "name": "SampleTest.TestA",
    "elapsed_time": "0.001"
  }
}
```

With `--jsonl`, the file is:

```text
data/events.jsonl
```

With `--sqlite-db`, the file is:

```text
data/events.db
```

The SQLite table is named `events` and stores:

- `id`
- `utc`
- `monotonic`
- `session_id`
- `client`
- `params`

## 4) Rebuild structured output from raw events

After the stream is captured, process the stored events into a nested JSON/XML summary:

```bash
python3 process_events.py \
  --jsonl-path ./data/events.jsonl \
  --output-json-path ./data/output.json \
  --output-xml-path ./data/output.xml
```

Or from SQLite:

```bash
python3 process_events.py \
  --db-path ./data/events.db \
  --output-json-path ./data/output.json \
  --output-xml-path ./data/output.xml
```

The `process_events.py` script reads event records, tracks the active session, and reconstructs the GoogleTest hierarchy:

- `testprogramstart` / `testprogramend`
- `testiterationstart` / `testiterationend`
- `testcasestart` / `testcaseend`
- `teststart` / `testend`
- `testpartresult`

It writes a JSON file and an XML file using naming based on the provided output paths, e.g.:

- `./data/output.json`
- `./data/output.xml`

The processor also expects the standard streaming protocol marker:

```text
gtest_streaming_protocol_version=1.0
```

It keeps track of protocol version and then reconstructs each session with start/end metadata and elapsed run times.

## 5) Quick smoke test

A minimal TCP payload can be sent with a tool such as `nc`:

```bash
printf '%s\n' 'gtest_streaming_protocol_version=1.0' 'event=testprogramstart&name=session_1' 'event=testcasestart&name=DemoSuite' 'event=teststart&name=DemoSuite.TestOne' 'event=testend&name=DemoSuite.TestOne&passed=true&elapsed_time=0.010' 'event=testcaseend&name=DemoSuite&passed=true&elapsed_time=0.020' 'event=testprogramend&name=session_1&passed=true&elapsed_time=0.030' | nc 127.0.0.1 8080
```

Then run the processor against the saved `events.jsonl` file to inspect the generated structured output.

## Files in this repository

- `gtest_stream_server.py`: TCP receiver and raw-event writer
- `result_store.py`: SQLite/JSONL storage layer
- `process_events.py`: event reassembler into structured output
- `my_logging.py`: shared logging setup

## Notes

This repo follows the raw GoogleTest streaming protocol rather than generating a finished summary in the receiver itself. The raw data remains the authoritative source, and the processor is responsible for reconstructing session/test hierarchy from it.
