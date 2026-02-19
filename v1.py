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
    last_err: str = ""


async def worker(name: int, sem: asyncio.Semaphore, host: str, port: int,
                 payload: bytes, connect_timeout: float, keepalive: float,
                 stats: Stats):
    while True:
        async with sem:
            stats.attempts += 1
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=connect_timeout
                )
                stats.connected += 1

                if payload:
                    writer.write(payload)
                    await writer.drain()
                    stats.sent += 1

                # keep connection open briefly (simulates “real clients”)
                if keepalive > 0:
                    await asyncio.sleep(keepalive)

                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass

            except Exception as e:
                stats.failed += 1
                stats.last_err = str(e)


async def rate_limiter(sem: asyncio.Semaphore, rps: int, concurrency: int, duration: float):
    """
    Controls how many "in-flight" operations can happen and paces starts to approx rps.
    """
    # start with full concurrency available
    for _ in range(concurrency):
        sem.release()

    if rps <= 0:
        # unlimited pacing: just run for duration
        await asyncio.sleep(duration)
        return

    interval = 1.0 / rps
    end = time.time() + duration
    while time.time() < end:
        # take one permit then immediately release it (gates the start rate)
        await sem.acquire()
        sem.release()
        await asyncio.sleep(interval)


async def main_async(args):
    sem = asyncio.Semaphore(0)  # we'll pre-fill in rate_limiter
    stats = Stats()

    if args.mode == "random":
        payload = os.urandom(args.size)
    elif args.mode == "http":
        # simple HTTP request; safe for your own HTTP service
        payload = (
            f"GET {args.path} HTTP/1.1\r\n"
            f"Host: {args.ip}\r\n"
            f"Connection: close\r\n\r\n"
        ).encode("utf-8")
    else:
        payload = b""

    tasks = []
    for i in range(args.workers):
        tasks.append(asyncio.create_task(
            worker(i, sem, args.ip, args.port, payload, args.connect_timeout, args.keepalive, stats)
        ))

    start = time.time()
    limiter = asyncio.create_task(rate_limiter(sem, args.rps, args.concurrency, args.duration))

    # periodic stats print
    try:
        while not limiter.done():
            await asyncio.sleep(1.0)
            elapsed = max(1e-6, time.time() - start)
            print(
                f"[{elapsed:6.1f}s] attempts={stats.attempts} "
                f"connected={stats.connected} sent={stats.sent} failed={stats.failed} "
                f"fail_rate={(stats.failed / max(1, stats.attempts)) * 100:5.1f}% "
                f"last_err={stats.last_err[:80]}"
            )
    finally:
        limiter.cancel()
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
    print(f"Approx attempts/sec: {stats.attempts / elapsed:.1f}")


def parse_args():
    p = argparse.ArgumentParser(description="Safe TCP load tester (for systems you own/operate).")
    p.add_argument("--ip", required=True)
    p.add_argument("--port", type=int, required=True)

    p.add_argument("--duration", type=float, default=15.0, help="seconds")
    p.add_argument("--concurrency", type=int, default=200, help="max in-flight ops")
    p.add_argument("--workers", type=int, default=500, help="async tasks")
    p.add_argument("--rps", type=int, default=200, help="target starts per second; 0 = unlimited pacing (not recommended)")

    p.add_argument("--connect-timeout", type=float, default=1.0)
    p.add_argument("--keepalive", type=float, default=0.05, help="seconds to keep a connection open")

    p.add_argument("--mode", choices=["connect_only", "random", "http"], default="connect_only")
    p.add_argument("--size", type=int, default=256, help="payload size in bytes for random mode")
    p.add_argument("--path", default="/", help="HTTP path for http mode")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main_async(args))
