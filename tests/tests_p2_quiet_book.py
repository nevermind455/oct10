#!/usr/bin/env python3
"""P2 (2026-10-14 repair): paper's "unknown book state". No network.

    python tests_p2_quiet_book.py

By default nothing changes: when the market feed has carried nothing stamped
after the modelled match, the book then is unknown and the order is refused.
SNIPER_BOOK_QUIET=1 (opt-in) takes a connected, gap-free, LIVE feed's last
book as the book then - labelled evidence="quiet", never on a disconnected,
gapped or stale feed, and never in place of a proven answer.
"""
from __future__ import annotations

import pathlib
import sys
import time
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import testing_env  # noqa: E402,F401 - first: tests never read the bot's .env
import sniper_mode as S  # noqa: E402
import tests_sniper_ioc as I  # noqa: E402
from feeds.book import BookState  # noqa: E402


def state(**kw):
    book = BookState(**kw)
    book.connected = True
    book.set_active(["A", "B"])
    now = int(time.time() * 1000)
    for token in book.active:
        assert book.apply_snapshot(token, [(0.4, 10)], [(0.6, 10)], ts_ms=now)
    return book, now


class BookLevel(unittest.TestCase):
    def test_default_is_unchanged_unknown(self):
        book, now = state()
        view, info = book.view_at("A", now + 50)
        self.assertIsNone(view)
        self.assertEqual(info["why"], "the feed has not yet passed that time")
        self.assertNotIn("evidence", info)

    def test_quiet_live_feed_gives_the_last_book_labelled(self):
        book, now = state()
        view, info = book.view_at("A", now + 50, allow_quiet=True)
        self.assertEqual((view.best_ask, info["evidence"], info["book_ts"]), (0.6, "quiet", now))

    def test_a_proven_answer_is_never_relabelled(self):
        book, now = state()
        book.note_exchange_ts(now + 100)
        a, ia = book.view_at("A", now + 50)
        b, ib = book.view_at("A", now + 50, allow_quiet=True)
        self.assertEqual((a.best_ask, b.best_ask), (0.6, 0.6))
        self.assertNotIn("evidence", ia)
        self.assertNotIn("evidence", ib)

    def test_never_on_a_disconnected_gapped_stale_or_inactive_feed(self):
        book, now = state()
        book.connected = False
        self.assertIsNone(book.view_at("A", now + 50, allow_quiet=True)[0])
        book, now = state()
        book.desync_all("reconnect")                       # a gap: the book is no longer known
        self.assertIsNone(book.view_at("A", now + 50, allow_quiet=True)[0])
        book, now = state(stale_after=0.05)
        time.sleep(0.12)                                   # nothing received for longer than stale_after
        self.assertIsNone(book.view_at("A", now + 50, allow_quiet=True)[0])
        book, now = state()
        self.assertIsNone(book.view_at("Z", now + 50, allow_quiet=True)[0])


class SniperLevel(I.PaperIOCBase):
    def arrival(self, **env):
        self.sn = S.Sniper(self.m, self.broker, settings=I.settings(SNIPER_BOOK_WAIT_MS="20", **env))
        book, now = state()
        self.m.book = book
        return self.sn._arrival_book("A", now + 50, time.monotonic())

    def test_off_by_default_the_order_is_unknown(self):
        self.assertFalse(I.settings().book_quiet)
        snap, info = self.arrival()
        self.assertIsNone(snap)
        self.assertEqual(info["why"], "the feed has not yet passed that time")

    def test_opt_in_fills_against_the_last_real_book_labelled(self):
        snap, info = self.arrival(SNIPER_BOOK_QUIET="1")
        self.assertIsNotNone(snap)
        self.assertEqual((float(snap.asks[0][0]), float(snap.asks[0][1])), (0.6, 10.0))   # the book as delivered
        self.assertEqual(info["evidence"], "quiet")
        live_like = I.settings(SNIPER_BOOK_QUIET="1", SNIPER_LATENCY_MS="")      # the start line names it
        self.assertIn("quiet feed's last book inferred", live_like.lines()[0])

    def test_opt_in_does_nothing_on_the_local_clock(self):
        snap, info = self.arrival(SNIPER_BOOK_QUIET="1", SNIPER_BOOK_CLOCK="local")
        self.assertNotIn("evidence", info)


if __name__ == "__main__":
    unittest.main()
