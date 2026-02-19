#!/usr/bin/env python3
import asyncio
import argparse
import os
import time
from dataclasses import dataclass


@dataclass
class Stats:
    attempts: int = 0
    connected: int = 0
    sent: int = 0
    failed: int = 0
    http_ok: int = 0
    http_bad: int = 0
    bytes_read: int = 0
    last_err: str = ""


class TokenBucket:
    """
    Simple async token bucket rate limiter.
    - Fill at `rate` tokens/sec
    - Capacity `burst`
    Each request must acquire 1 token.
    """
    def __init__(self, rate: float, burst: int):
        self.rate = float(rate)
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        if self.rate <= 0:
            return  # unlimited pacing

        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = now - self.updated
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.updated = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                # need to wait for enough tokens
                needed = 1.0 - self.tokens
                wait_s = max(needed / self.rate, 0.001)

            await asyncio.sleep(wait_s)


def build_http_request(ip: str, path: str, method: str, body: bytes,
                       headers: list[str] | None, keepalive: bool) -> bytes:
    method_u = method.upper()
    if not path.startswith("/"):
        path = "/" + path

    base = [
        f"{method_u} {path} HTTP/1.1",
        f"Host: {ip}",
        "User-Agent: tcp-load-tester/2.0",
    ]

    if method_u in ("POST", "PUT", "PATCH"):
        base.append("Content-Type: application/json")
        base.append(f"Content-Length: {len(body)}")
    else:
        # no body for GET/HEAD by default
        body = b""

    base.append(f"Connection: {'keep-alive' if keepalive else 'close'}")

    if headers:
        for h in headers:
            if h.strip():
                base.append(h.strip())

    req = ("\r\n".join(base) + "\r\n\r\n").encode("utf-8") + body
    return req


async def one_attempt(host: str, port: int, payload: bytes,
                      connect_timeout: float,
                      read_response: bool,
                      read_limit: int,
                      keepalive_sleep: float,
                      stats: Stats) -> None:
    stats.attempts += 1
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=connect_timeout,
        )
        stats.connected += 1

        if payload:
            writer.write(payload)
            await writer.drain()
            stats.sent += 1

        # Read just enough to confirm response & apply backpressure realistically
        if read_response:
            try:
                data = await asyncio.wait_for(reader.read(read_limit), timeout=connect_timeout)
                if data:
                    stats.bytes_read += len(data)
                    # Basic HTTP status check if it looks like HTTP
                    if data.startswith(b"HTTP/"):
                        # e.g. HTTP/1.1 200 OK
                        parts = data.split(b" ", 2)
                        if len(parts) >= 2 and parts[1].isdigit():
                            code = int(parts[1])
                            if 200 <= code < 400:
                                stats.http_ok += 1
                            else:
                                stats.http_bad += 1
            except Exception:
                # response read failed; still counts as attempt, likely timeout/close
                pass

        if keepalive_sleep > 0:
            await asyncio.sleep(keepalive_sleep)

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    except Exception as e:
        stats.failed += 1
        # TimeoutError often stringifies to empty; keep type + repr.
        stats.last_err = f"{type(e).__name__}: {repr(e)}"


async def worker(worker_id: int,
                 host: str,
                 port: int,
                 payload: bytes,
                 connect_timeout: float,
                 read_response: bool,
                 read_limit: int,
                 keepalive_sleep: float,
                 concurrency_sem: asyncio.Semaphore,
                 bucket: TokenBucket,
                 stop_event: asyncio.Event,
                 stats: Stats) -> None:
    while not stop_event.is_set():
        # Enforce rate *before* taking concurrency slot so we don't hoard FDs.
        await bucket.acquire()

        try:
            await asyncio.wait_for(concurrency_sem.acquire(), timeout=1.0)
        except Exception:
            continue

        try:
            await one_attempt(
                host=host,
                port=port,
                payload=payload,
                connect_timeout=connect_timeout,
                read_response=read_response,
                read_limit=read_limit,
                keepalive_sleep=keepalive_sleep,
                stats=stats,
            )
        finally:
            concurrency_sem.release()


async def main_async(args) -> None:
    stats = Stats()

    # Build payload
    if args.mode == "random":
        payload = os.urandom(args.size)
    elif args.mode == "http":
        body = (args.body or "").encode("utf-8")
        keepalive_header = args.http_keepalive and args.keepalive > 0
        payload = build_http_request(
            ip=args.ip,
            path=args.path,
            method=args.method,
            body=body,
            headers=args.header,
            keepalive=keepalive_header,
        )
    else:
        payload = b""

    # Concurrency cap
    concurrency_sem = asyncio.Semaphore(max(1, args.concurrency))

    # Token bucket for true RPS control
    # burst defaults to rps (or concurrency if rps<=0)
    burst = args.burst if args.burst is not None else (args.rps if args.rps > 0 else args.concurrency)
    bucket = TokenBucket(rate=args.rps, burst=int(max(1, burst)))

    stop_event = asyncio.Event()

    tasks = []
    for i in range(args.workers):
        tasks.append(asyncio.create_task(
            worker(
                worker_id=i,
                host=args.ip,
                port=args.port,
                payload=payload,
                connect_timeout=args.connect_timeout,
                read_response=args.read_response,
                read_limit=args.read_limit,
                keepalive_sleep=args.keepalive,
                concurrency_sem=concurrency_sem,
                bucket=bucket,
                stop_event=stop_event,
                stats=stats,
            )
        ))

    start = time.time()
    end_time = start + args.duration

    try:
        while time.time() < end_time:
            await asyncio.sleep(1.0)
            elapsed = max(1e-6, time.time() - start)
            attempts_per_s = stats.attempts / elapsed
            fail_rate = (stats.failed / max(1, stats.attempts)) * 100.0

            extra = ""
            if args.mode == "http" and args.read_response:
                extra = f" http_ok={stats.http_ok} http_bad={stats.http_bad} bytes_read={stats.bytes_read}"

            print(
                f"[{elapsed:6.1f}s] attempts={stats.attempts} connected={stats.connected} "
                f"sent={stats.sent} failed={stats.failed} fail_rate={fail_rate:5.1f}% "
                f"attempts/sec={attempts_per_s:7.1f}{extra} last_err={stats.last_err[:120]}"
            )
    finally:
        stop_event.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = max(1e-6, time.time() - start)
    print("\n=== Final ===")
    print(f"Elapsed: {elapsed:.2f}s")
    print(f"Attempts: {stats.attempts}")
    print(f"Connected: {stats.connected}")
    print(f"Sent: {stats.sent}")
    print(f"Failed: {stats.failed}")
    if args.mode == "http" and args.read_response:
        print(f"HTTP OK: {stats.http_ok}")
        print(f"HTTP Bad: {stats.http_bad}")
        print(f"Bytes read: {stats.bytes_read}")
    print(f"Approx attempts/sec: {stats.attempts / elapsed:.1f}")


def parse_args():
    p = argparse.ArgumentParser(description="Safe TCP/HTTP load tester (for systems you own/operate).")
    p.add_argument("--ip", required=True, help="Target IP/host")
    p.add_argument("--port", type=int, required=True, help="Target port")

    p.add_argument("--duration", type=float, default=15.0, help="seconds to run")
    p.add_argument("--concurrency", type=int, default=200, help="max in-flight operations")
    p.add_argument("--workers", type=int, default=500, help="number of async workers")

    p.add_argument("--rps", type=float, default=200.0, help="target request starts per second; 0 = unlimited")
    p.add_argument("--burst", type=int, default=None, help="token bucket burst capacity (default: rps)")

    p.add_argument("--connect-timeout", type=float, default=1.0, help="seconds")

    p.add_argument("--keepalive", type=float, default=0.0,
                   help="seconds to keep connection open after sending/reading (0 closes immediately)")

    p.add_argument("--mode", choices=["connect_only", "random", "http"], default="connect_only")
    p.add_argument("--size", type=int, default=256, help="payload size (bytes) for random mode")

    # HTTP options
    p.add_argument("--path", default="/", help="HTTP path")
    p.add_argument("--method", default="GET", help="HTTP method (GET/POST/PUT/PATCH)")
    p.add_argument("--body", default='{"input":"test"}', help="HTTP body (JSON string) for POST/PUT/PATCH")
    p.add_argument("--header", action="append", default=[], help='Extra header, e.g. --header "Authorization: Bearer X"')
    p.add_argument("--read-response", action="store_true",
                   help="Read up to --read-limit bytes of response (recommended for realism)")
    p.add_argument("--read-limit", type=int, default=4096, help="Max bytes to read from response")

    # If you want keep-alive header when keepalive>0
    p.add_argument("--http-keepalive", action="store_true",
                   help="Send Connection: keep-alive when keepalive>0 (otherwise Connection: close)")

    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main_async(args))
