#!/usr/bin/env python3
"""Shows the engine doing real work, through the release binary and a throwaway directory:

  1. basic operations
  2. a heavy write load, with flushes and compactions all the way down to L3
  3. every key read back and checked against what was acknowledged
  4. kill -9 in the middle of writes, then recovery
  5. a single flipped bit in an SST is caught, never returned as data
  6. a torn WAL under both recovery modes
  7. deletes are dropped once they reach the bottom level, so the number of SSTs goes down

  python3 scripts/demo.py                run it
  PAUSE=1 python3 scripts/demo.py        wait for Enter between steps, for walking through it live
  OPS=200000 python3 scripts/demo.py     lighter load that stops at L2 (default: 1,000,000 operations over 200,000 keys)
"""

import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OPS = int(os.environ.get("OPS", 1_000_000))
KEYS = int(os.environ.get("KEYS", 200_000))
BIN = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")) / "release" / "database-engine"

DB_ARGS = ["--demo", "--sync", "every:100"]


SIZES = ["--memtable-size", "1MiB", "--max-sst-size", "1MiB", "--max-bytes-for-level-base", "4MiB"]

# step 7 shrinks the levels further (L1 160 KiB, L2 320 KiB), so most of its tombstones make it down to L3
SMALL_LEVELS = [
    "--memtable-size", "160KiB",
    "--max-sst-size", "160KiB",
    "--max-bytes-for-level-base", "160KiB",
    "--level-multiplier", "2",
    "--l0-compaction-trigger", "2",
]
DROP_KEYS = 100_000

if sys.stdout.isatty():
    BOLD, DIM, RESET = "\033[1m", "\033[2m", "\033[0m"
else:
    BOLD = DIM = RESET = ""

PAD = "x" * 90
RECOVERED = re.compile(r"Recovered \d+ records from \d+ WAL files")
STATS = re.compile(r"files_per_level: \[(.*?)\], bytes_per_level: \[(.*?)\]")


def step(title, description):
    if os.environ.get("PAUSE"):
        input(f"{DIM}press Enter for the next step{RESET}")
    print(f"\n{BOLD}== {title} =={RESET}")
    print(f"{DIM}{description}{RESET}\n")



def cmd(i):
    """the i-th write of the workload. 10% of them are deletes"""
    k = (i * 7919 + 13) % KEYS
    if i % 10 == 9:
        return f"del key{k:07d}"
    return f"put key{k:07d} v{i}-{PAD}"


def apply(state, command):
    op, key, *value = command.split(" ", 2)
    if op == "put":
        state[key] = value[0]
    else:
        state.pop(key, None)


def expected_values(state):
    """what `get` should print for every key, in key order"""
    return [state.get(f"key{i:07d}", "(None)") for i in range(KEYS)]




def engine(directory, *args):
    # step 7 passes its own, smaller sizes, and clap rejects a flag given twice
    sizes = [] if "--memtable-size" in args else SIZES
    return [str(BIN), *DB_ARGS, *sizes, "--dir", str(directory), *args]


def run_session(directory, commands, *args):
    """runs one REPL session with all the commands. returns what recovery printed on open (or None), and the replies"""
    result = subprocess.run(
        engine(directory, *args),
        input="".join(c + "\n" for c in commands),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.exit(f"the engine failed:\n{result.stdout}{result.stderr}")
    lines = result.stdout.splitlines()
    if lines and RECOVERED.fullmatch(lines[0]):
        return lines[0], lines[1:]
    return None, lines


def next_line(proc):
    line = proc.stdout.readline()
    if not line:
        sys.exit("the engine exited before replying to every command")
    return line.rstrip("\n")


class Repl:
    """a REPL session that stays open between commands, for a step that has to wait on background work"""

    def __init__(self, directory, *args):
        self.proc = subprocess.Popen(
            engine(directory, *args), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
        )
        self.lines = queue.Queue()
        # replies are read on their own thread, so sending thousands of commands at once can't deadlock
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.proc.stdout:
            self.lines.put(line.rstrip("\n"))
        self.lines.put(None)

    def send(self, commands):
        self.proc.stdin.write("".join(c + "\n" for c in commands))
        self.proc.stdin.flush()

    def reply(self):
        line = self.lines.get()
        if line is None:
            sys.exit("the engine exited unexpectedly")
        return line

    def ask(self, command):
        self.send([command])
        return self.reply()

    def close(self):
        self.proc.stdin.close()  # EOF ends the REPL, and the engine closes cleanly
        self.proc.wait()


def read_all_keys(directory):
    """reads every key through a fresh REPL session, and prints what recovery did on open"""
    start = time.monotonic()
    recovered, values = run_session(directory, (f"get key{i:07d}" for i in range(KEYS)))
    if recovered and not recovered.startswith("Recovered 0 records"):
        print(f"    on open: {recovered}")
    print(f"    read {KEYS:,} keys in {time.monotonic() - start:.1f}s")
    return values


def report_mismatch(expected, actual):
    print("    MISMATCH:")
    if len(actual) != len(expected):
        print(f"    expected {len(expected):,} replies, got {len(actual):,}")
    shown = 0
    for i, (want, got) in enumerate(zip(expected, actual)):
        if want != got:
            print(f"    key{i:07d}: expected {want[:40]!r}, got {got[:40]!r}")
            shown += 1
            if shown == 5:
                break
    sys.exit(1)


def parse_stats(line):
    """(files per level, bytes per level) from the reply to `stats`"""
    match = STATS.search(line)
    if not match:
        sys.exit(f"expected stats, got: {line}")
    files = tuple(int(n) for n in match.group(1).split(", "))
    sizes = tuple(int(n) for n in match.group(2).split(", "))
    return files, sizes


def tree_line(label, stats):
    files, sizes = stats
    return (
        f"    {label}    L0 {files[0]:3}   L1 {files[1]:3}   L2 {files[2]:3}   L3 {files[3]:3} files"
        f"    {sum(sizes) / 2**20:6.1f} MiB in SSTs"
    )


# the steps


def build():
    print(f"{BOLD}Building the release binary{RESET}")
    # compiler warnings would bury the demo, so the build output is only shown if it fails
    result = subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(ROOT / "Cargo.toml")],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stdout + result.stderr)
        sys.exit(1)


def basic_operations(directory):
    step("1. Basic operations", "One REPL session: writes, overwrites, deletes and reads.")
    directory.mkdir()
    commands = [
        "put apple red", "get apple", "put apple green", "get apple", "del apple", "get apple",
        "get never-written", "put greeting hello world", "get greeting",
    ]
    _, replies = run_session(directory, commands)
    for command, reply in zip(commands, replies):
        print(f"    > {command:<28} {reply}")


def heavy_write_load(directory, work):
    step(
        "2. Heavy write load",
        f"{OPS:,} operations over {KEYS:,} keys: ~100 byte values, every key overwritten\n"
        "several times, 10% deletes. Each line below is the tree after another 10% of the operations.",
    )
    directory.mkdir()
    every = max(OPS // 10, 1)
    load = work / "load.txt"
    with open(load, "w") as f:
        for i in range(OPS):
            f.write(cmd(i) + "\n")
            if (i + 1) % every == 0:
                f.write("stats\n")

    acked = bytearray(OPS)  # acked[i] is 1 if the engine replied OK to cmd(i)
    refused = 0
    start = time.monotonic()
    with open(load) as commands:
        proc = subprocess.Popen(engine(directory), stdin=commands, stdout=subprocess.PIPE, text=True)
    for i in range(OPS):
        reply = next_line(proc)
        if reply == "OK":
            acked[i] = 1
        elif reply.startswith("error: Writes are stalled"):
            refused += 1
        if (i + 1) % every == 0:
            print(tree_line(f"{i + 1:>10,} ops", parse_stats(next_line(proc))))
    proc.wait()
    elapsed = time.monotonic() - start

    written = sum(acked)
    files = list(directory.iterdir())
    ssts = sum(p.suffix == ".sst" for p in files)
    wals = sum(p.suffix == ".wal" for p in files)
    total = sum(p.stat().st_size for p in files) / 2**20
    print()
    print(
        f"    {written:,} writes acknowledged in {elapsed:.1f}s, {int(written / elapsed):,} per second"
        " (including close, which waits for background work)"
    )
    print(f"    {refused:,} refused with WritesStalled (L0 hit its stop trigger, and the REPL does not retry)")
    print(f"    on disk: {ssts} SST files, {wals} WAL, MANIFEST and LOCK, {total:.1f} MiB in total")
    return acked


def check_every_key(directory, acked):
    step(
        "3. Every key checked",
        "The expected value of each key is rebuilt from the acknowledged writes only, then all keys are\n"
        "read back from a reopened engine.",
    )
    state = {}
    for i in range(OPS):
        if acked[i]:
            apply(state, cmd(i))
    expected = expected_values(state)
    actual = read_all_keys(directory)
    if actual != expected:
        report_mismatch(expected, actual)
    print(f"    {BOLD}all {KEYS:,} keys match{RESET}")
    return expected


def kill_mid_writes(directory):
    step(
        "4. kill -9 in the middle of writes",
        "Writes stream in as fast as the engine takes them, and the process is killed 2 seconds after its\n"
        "first reply. Every write it acknowledged must be there after a restart.",
    )
    directory.mkdir()
    proc = subprocess.Popen(engine(directory), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    acked = []  # acked[i] is True if the engine replied OK to cmd(i)
    first_reply = threading.Event()

    def feed():
        i = 0
        try:
            while True:  # until the kill breaks the pipe
                proc.stdin.write("".join(cmd(n) + "\n" for n in range(i, i + 1000)))
                i += 1000
        except (OSError, ValueError):
            pass

    def collect():
        for line in proc.stdout:
            if not line.endswith("\n"):
                break  # cut off by the kill, so it was never a complete reply
            acked.append(line == "OK\n")
            first_reply.set()

    threading.Thread(target=feed, daemon=True).start()
    reader = threading.Thread(target=collect)
    reader.start()
    # the countdown starts at the first reply, so a slow start can't use up the 2 seconds
    if not first_reply.wait(timeout=30):
        proc.kill()
        sys.exit("the engine didn't reply within 30s")
    time.sleep(2)
    proc.kill()  # SIGKILL
    proc.wait()
    reader.join()
    try:
        proc.stdin.close()
    except (OSError, ValueError):
        pass  # the pipe is already broken
    print(f"    killed after {sum(acked):,} acknowledged writes")

    state = {}
    for i, ok in enumerate(acked):
        if ok:
            apply(state, cmd(i))
    without_in_flight = expected_values(state)
    # the write after the last reply may or may not have reached the WAL before the kill, so both outcomes are valid
    apply(state, cmd(len(acked)))
    with_in_flight = expected_values(state)

    actual = read_all_keys(directory)
    if actual != without_in_flight and actual != with_in_flight:
        report_mismatch(without_in_flight, actual)
    print(f"    {BOLD}all {KEYS:,} keys match the acknowledged writes{RESET}")


def flipped_bit(directory, expected):
    step(
        "5. A flipped bit is caught",
        "One bit is flipped inside the first data block of the newest SST from step 2. Every block\n"
        "carries a CRC32, so a read that lands on it fails loudly instead of returning wrong data.",
    )
    # the newest SST is the L0 file the last reopen replayed from the WAL: it holds the newest version of each
    # of its keys and L0 is searched first, so reading those keys has to go through its blocks
    victim = max(directory.glob("*.sst"), key=lambda p: p.stat().st_mtime_ns)
    with open(victim, "r+b") as f:
        f.seek(100)
        byte = f.read(1)[0]
        f.seek(100)
        f.write(bytes([byte ^ 1]))
    print(f"    flipped bit 0 of byte 100 in {victim.name}")

    actual = read_all_keys(directory)
    correct = caught = wrong = 0
    sample = None
    for want, got in zip(expected, actual):
        if got == want:
            correct += 1
        elif got.startswith("error: data corrupted"):
            caught += 1
            sample = sample or got
        else:
            wrong += 1
    print(f"    {correct:,} keys correct, {caught:,} reads stopped by a checksum error, {wrong:,} returned wrong data")
    if sample:
        print(f"    e.g. {sample.split(' (occurred at')[0]}")
    if wrong:
        sys.exit(1)


def torn_wal(directory):
    step(
        "6. A torn WAL",
        "1,000 writes, then the last 3 bytes of the WAL are cut off, as if the process died mid-append.\n"
        "absolute-consistency refuses to open; point-in-time opens with everything before the damage.",
    )
    directory.mkdir()
    run_session(directory, [f"put wal-key{i:04d} value{i}" for i in range(1000)])
    (wal,) = directory.glob("*.wal")
    os.truncate(wal, wal.stat().st_size - 3)

    print("    --wal-recovery-mode absolute-consistency:")
    result = subprocess.run(
        engine(directory, "--wal-recovery-mode", "absolute-consistency"),
        input="get wal-key0999\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    for line in result.stdout.splitlines():
        print("      " + re.sub(r'file_path: "[^"]*"', "file_path: ...", line))

    print("    --wal-recovery-mode point-in-time:")
    commands = ["get wal-key0998", "get wal-key0999"]
    recovered, replies = run_session(directory, commands, "--wal-recovery-mode", "point-in-time")
    if recovered:
        print(f"      on open: {recovered}")
    for command, reply in zip(commands, replies):
        print(f"      > {command:<18} {reply}")
    (corrupt,) = directory.glob("*.wal.corrupt")
    print(f"      damaged WAL kept as {corrupt.name}")



def write_all(repl, commands):
    """sends every command, then sends again the ones refused with WritesStalled until none are left"""
    while commands:
        repl.send(commands)
        refused = []
        for command in commands:
            reply = repl.reply()
            if reply.startswith("error: Writes are stalled"):
                refused.append(command)
            elif reply != "OK":
                sys.exit(f"unexpected reply to {command!r}: {reply}")
        commands = refused
        if commands:
            time.sleep(0.05)


def settle(repl, quiet=1.0, limit=120):
    """asks for stats until they stop changing. every `stats` runs maintenance, which installs finished background
    work and starts the next compaction, so this drives compaction until no level is over its size target"""
    start = changed_at = time.monotonic()
    last = None
    while True:
        stats = parse_stats(repl.ask("stats"))
        now = time.monotonic()
        if stats != last:
            last, changed_at = stats, now
        elif now - changed_at >= quiet or now - start >= limit:
            return stats
        time.sleep(0.02)


def deletes_dropped_at_bottom(directory):
    step(
        "7. Deletes are dropped at the bottom level",
        f"{DROP_KEYS:,} keys with 1-byte values are written and left to settle, mostly into L3, then every one of\n"
        "them is deleted. A tombstone is about as big as the record it deletes, so swapping values for tombstones\n"
        "can't shrink the tree. It shrinks because a tombstone that reaches L3 is dropped together with the value\n"
        "it deletes. This step uses smaller levels than the other steps, so most of the tombstones make it down to L3.",
    )
    directory.mkdir()
    repl = Repl(directory, *SMALL_LEVELS)
    keys = [f"key{i:07d}" for i in range(DROP_KEYS)]

    write_all(repl, [f"put {k} v" for k in keys])
    before = settle(repl)
    print(tree_line(f"{DROP_KEYS:,} puts, settled".rjust(22), before))
    chunk = DROP_KEYS // 5
    for done in range(chunk, DROP_KEYS + 1, chunk):
        write_all(repl, [f"del {k}" for k in keys[done - chunk : done]])
        print(tree_line(f"{done:,} deletes".rjust(22), parse_stats(repl.ask("stats"))))
    after = settle(repl)
    print(tree_line("settled".rjust(22), after))

    repl.send([f"get {k}" for k in keys])
    still_there = sum(repl.reply() != "(None)" for _ in keys)
    repl.close()

    (files_before, bytes_before), (files_after, bytes_after) = before, after
    print()
    print(
        f"    L3 went from {files_before[3]} files ({bytes_before[3] / 2**20:.1f} MiB) to {files_after[3]}"
        f" ({bytes_after[3] / 2**20:.1f} MiB), and the whole tree from {sum(files_before)} SSTs to {sum(files_after)}"
    )
    if still_there:
        sys.exit(f"    {still_there:,} deleted keys still returned a value")
    print(f"    {BOLD}all {DROP_KEYS:,} deleted keys read back as (None){RESET}")
    if bytes_after[3] >= bytes_before[3] or sum(files_after) >= sum(files_before):
        sys.exit("    expected L3 and the number of SSTs to go down")


def main():
    sys.stdout.reconfigure(line_buffering=True)  # progress lines show up as they're printed, even through a pipe
    build()
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        load_db = work / "load"
        basic_operations(work / "basic")
        acked = heavy_write_load(load_db, work)
        expected = check_every_key(load_db, acked)
        kill_mid_writes(work / "crash")
        flipped_bit(load_db, expected)
        torn_wal(work / "wal")
        deletes_dropped_at_bottom(work / "drop")
    print(f"\n{BOLD}Done.{RESET}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
