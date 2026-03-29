#!/usr/bin/env python3
"""
Adaptive async load generator with curses interface.
Graph increments by 1 bar per refresh. Stable fixed-width layout.
UI refresh = 0.33s.
"""

from __future__ import annotations
import asyncio
import aiohttp
import argparse
import curses
import os
import time
import uuid
import random
from collections import deque
from typing import List

# ================================
# CONFIG
# ================================
UI_INTERVAL = 0.33
ADAPT_INTERVAL = 4.0
GRAPH_HISTORY = 200           # exactly 200 characters wide history
DEFAULT_MIN_CHUNK = 16 * 1024
DEFAULT_MAX_CHUNK = 4 * 1024 * 1024
MAX_MULTIPLIER = 4

# ================================
# HELPERS
# ================================
def human_bytes(n: float) -> str:
    if n is None:
        return "   N/A   "
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    return f"{n:7.2f} {units[i]:<3}"

def human_bits(n: float) -> str:
    if n is None:
        return "    N/A     "
    # Convert bytes to bits
    n *= 8
    units = ["bps", "Kbps", "Mbps", "Gbps", "Tbps"]
    i = 0
    while n >= 1000 and i < len(units) - 1:
        n /= 1000.0
        i += 1
    return f"{n:7.2f} {units[i]:<4}"

SPARK = "▁▂▃▄▅▆▇█"

def spark_step(prev_max: float, prev_min: float, value: float):
    """Return one sparkline character for the given instantaneous value."""
    mx = max(prev_max, value, 1e-9)
    mn = min(prev_min, value)
    rng = max(mx - mn, 1e-9)
    idx = int((value - mn) / rng * (len(SPARK) - 1))
    idx = max(0, min(idx, len(SPARK) - 1))
    return SPARK[idx], mx, mn


# ================================
# STATS
# ================================
class WorkerStats:
    def __init__(self):
        self.total_bytes = 0
        self.instant_bps = 0.0
        self.moving_bps = None
        self.chunk_size = 64 * 1024
        self.errors = 0
        self.active = False

        # graph history (one char per refresh)
        self.graph = deque(maxlen=GRAPH_HISTORY)
        self.graph_max = 1.0
        self.graph_min = 0.0

    def record_chunk(self, nbytes: int, dt: float):
        self.total_bytes += nbytes
        if dt <= 0:
            return
        inst = nbytes / dt
        self.instant_bps = inst

        alpha = 0.35
        if self.moving_bps is None:
            self.moving_bps = inst
        else:
            self.moving_bps = alpha * inst + (1 - alpha) * self.moving_bps

    def adjust_chunk(self, min_chunk: int, max_chunk: int):
        if self.instant_bps <= 0:
            return
        t = self.chunk_size / max(self.instant_bps, 1e-9)
        if t < 0.02:
            self.chunk_size = int(min(max_chunk, self.chunk_size * 1.5))
        elif t > 0.6:
            self.chunk_size = int(max(min_chunk, self.chunk_size * 0.6))
        self.chunk_size = max(min_chunk, min(max_chunk, self.chunk_size))

    def step_graph(self):
        """Generate one new spark character per refresh."""
        ch, mx, mn = spark_step(self.graph_max, self.graph_min, self.instant_bps)
        self.graph.append(ch)
        self.graph_max = mx
        self.graph_min = mn


# ================================
# DOWNLOADER
# ================================
async def downloader(worker_id, url, session, stats: WorkerStats,
                     min_chunk, max_chunk, stop_event):
    backoff = 1.0
    while not stop_event.is_set():
        stats.active = True
        u = f"{url}{'&' if '?' in url else '?'}nocache={uuid.uuid4().hex}"
        try:
            async with session.get(u) as resp:
                resp.raise_for_status()
                backoff = 1.0
                while not stop_event.is_set():
                    t0 = time.time()
                    block = await resp.content.read(stats.chunk_size)
                    t1 = time.time()
                    if not block:
                        break
                    stats.record_chunk(len(block), t1 - t0)
                    stats.adjust_chunk(min_chunk, max_chunk)
            await asyncio.sleep(0.02)
        except asyncio.CancelledError:
            break
        except Exception:
            stats.errors += 1
            await asyncio.sleep(backoff + random.random() * 0.3)
            backoff = min(30.0, backoff * 1.4)
        finally:
            stats.active = False
            await asyncio.sleep(0.02)


# ================================
# CONCURRENCY MANAGER
# ================================
class ConcurrencyManager:
    def __init__(self, base, max_allowed):
        self.base = base
        self.max_allowed = max_allowed
        self.current = base
        self.direction = 0
        self.last_agg = 0.0

    def propose(self):
        if self.direction >= 0 and self.current < self.max_allowed:
            return self.current + 1, +1
        if self.direction < 0 and self.current > 1:
            return self.current - 1, -1
        return self.current, 0

    def evaluate(self, agg):
        if self.direction == 0:
            self.last_agg = agg
            return
        if agg > self.last_agg * 1.03:
            self.last_agg = agg
        else:
            if self.direction == 1 and self.current > 1:
                self.current -= 1
                self.direction = -1
            elif self.direction == -1 and self.current < self.max_allowed:
                self.current += 1
                self.direction = 1
            else:
                self.direction = 0
            self.last_agg = agg


# ================================
# DASHBOARD
# ================================
class Dashboard:
    def __init__(self, stdscr, stats_list, cm):
        self.stdscr = stdscr
        self.stats = stats_list
        self.cm = cm
        self.h = 0
        self.w = 0
        self.colors = curses.has_colors()

        if self.colors:
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)   # header
            curses.init_pair(2, curses.COLOR_YELLOW, -1) # summary
            curses.init_pair(3, curses.COLOR_GREEN, -1)  # active
            curses.init_pair(4, curses.COLOR_WHITE, -1)  # inactive
            curses.init_pair(5, curses.COLOR_RED, -1)    # errors
            curses.init_pair(6, curses.COLOR_MAGENTA, -1) # graph

    def size(self):
        self.h, self.w = self.stdscr.getmaxyx()

    def add(self, y, x, text, attr=0):
        if y < 0 or y >= self.h:
            return
        try:
            self.stdscr.addstr(y, x, text[:max(0, self.w - x - 1)], attr)
        except curses.error:
            pass

    def draw(self):
        self.size()
        self.stdscr.erase()

        # Header
        head = " Adaptive Async Load Generator (q=quit, +/- concurrency) "
        cx = max(0, (self.w - len(head)) // 2)
        self.add(0, cx, head,
            curses.color_pair(1) | curses.A_BOLD if self.colors else curses.A_REVERSE)

        # aggregated stats
        total_bytes = sum(s.total_bytes for s in self.stats)
        total_inst = sum(s.instant_bps for s in self.stats)
        total_ema = sum((s.moving_bps or 0) for s in self.stats)

        # line 2: concurrency
        self.add(
            2, 1,
            f"Concurrency: {self.cm.current} (base {self.cm.base}, max {self.cm.max_allowed})",
            curses.color_pair(2) | curses.A_BOLD if self.colors else 0
        )

        # line 3: bandwidth
        self.add(
            3, 1,
            f"Bandwidth  Inst:{human_bits(total_inst)}  EMA:{human_bits(total_ema)}  Total:{human_bytes(total_bytes)}",
            curses.color_pair(2) if self.colors else 0
        )

        # Worker table header
        row = 5
        self.add(row, 1, "ID  Act  Inst          EMA           Chunk   Err  Graph")
        row += 1

        # fixed column widths
        # ID=3, Act=3, Inst=12, EMA=12, Chunk=8, Err=5
        for i, s in enumerate(self.stats[: self.h - row - 2]):
            inst = human_bits(s.instant_bps)
            ema = human_bits(s.moving_bps or 0)
            chunk = f"{s.chunk_size//1024:5d}K"
            err = f"{s.errors:4d}"

            act = "●" if s.active else "○"
            if self.colors:
                if s.errors:
                    a = curses.color_pair(5)
                elif s.active:
                    a = curses.color_pair(3)
                else:
                    a = curses.color_pair(4)
            else:
                a = 0

            line = (
                f"{i:02d}  {act}   "
                f"{inst:<12}  "
                f"{ema:<12}  "
                f"{chunk:<8}  "
                f"{err:<5}  "
            )

            self.add(row, 1, line, a)

            # graph
            graph = "".join(s.graph)
            if self.colors:
                self.add(row, len(line) + 2, graph, curses.color_pair(6))
            else:
                self.add(row, len(line) + 2, graph)

            row += 1

        self.stdscr.refresh()


# ================================
# ORCHESTRATOR
# ================================
async def orchestrator(stdscr, args):
    curses.curs_set(0)
    stdscr.nodelay(True)

    base = args.concurrency
    max_allowed = args.max_concurrency or (base * MAX_MULTIPLIER)
    cm = ConcurrencyManager(base, max_allowed)

    stats: List[WorkerStats] = []
    tasks: List[asyncio.Task] = []
    stop = asyncio.Event()

    connector = aiohttp.TCPConnector(
        limit_per_host=max_allowed,
        ssl=not args.insecure
    )
    async with aiohttp.ClientSession(connector=connector) as session:

        # start initial workers
        for _ in range(cm.current):
            s = WorkerStats()
            stats.append(s)
            wid = len(stats) - 1
            tasks.append(asyncio.create_task(
                downloader(wid, args.url, session, s,
                           args.min_chunk, args.max_chunk, stop)))

        dash = Dashboard(stdscr, stats, cm)
        last_adapt = time.time()

        while not stop.is_set():
            # input handling
            c = stdscr.getch()
            if c == ord("q"):
                stop.set()
                break
            elif c in (ord("+"), ord("=")):
                if cm.current < max_allowed:
                    cm.current += 1
                    cm.direction = 0
            elif c == ord("-"):
                if cm.current > 1:
                    cm.current -= 1
                    cm.direction = 0

            # adjust worker count
            while len(stats) < cm.current:
                s = WorkerStats()
                stats.append(s)
                wid = len(stats) - 1
                tasks.append(asyncio.create_task(
                    downloader(wid, args.url, session, s,
                               args.min_chunk, args.max_chunk, stop)))
            while len(stats) > cm.current:
                t = tasks.pop()
                t.cancel()
                stats.pop()

            # update graphs
            for s in stats:
                s.step_graph()

            # adaptive concurrency
            now = time.time()
            if now - last_adapt >= ADAPT_INTERVAL:
                agg = sum((s.moving_bps or 0) for s in stats)
                cm.evaluate(agg)
                if cm.direction == 0:
                    newc, d = cm.propose()
                    if newc != cm.current:
                        cm.current = newc
                        cm.direction = d
                last_adapt = now

            # draw UI
            dash.draw()

            await asyncio.sleep(UI_INTERVAL)

        stop.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


# ================================
# CLI
# ================================
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--url", required=True)
    p.add_argument("-c", "--concurrency", type=int, default=os.cpu_count() or 4)
    p.add_argument("--max-concurrency", type=int, default=None)
    p.add_argument("--min-chunk", type=int, default=DEFAULT_MIN_CHUNK)
    p.add_argument("--max-chunk", type=int, default=DEFAULT_MAX_CHUNK)
    p.add_argument("--insecure", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    curses.wrapper(lambda stdscr: asyncio.run(orchestrator(stdscr, args)))


if __name__ == "__main__":
    main()