"""Minimum-entry attempt slots piggyback on existing durable live intents.

Offline: no SDK signing, credentials, exchange connections or real orders.
"""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import testing_env  # noqa: F401
from accounting.ledger import Ledger
import sniper_live as L


class MinimumEntryState(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'canary.json'
        self.ledger = Ledger(str(Path(self.temp.name) / 'ledger.json'))
        self.ledger.save()
        self.sessions = []
        self.end = time.time() + 240
        self.boot()

    def tearDown(self):
        for session in self.sessions:
            session.close()
        self.temp.cleanup()

    def boot(self):
        self.limits = L.CanaryLimits(max_order_shares=100, max_orders=100,
                                    max_fills=100, max_rounds=100,
                                    max_exposure_usd=1000, max_daily_loss_usd=1000)
        self.kill = L.KillSwitch()
        self.session = L.CanarySession(self.path, 'offline-wallet')
        self.session.open(self.limits, self.kill, self.ledger)
        self.sessions.append(self.session)

    def prepared(self, key, *, marker=True, condition='condition', token='up', end=None):
        p = L.Prepared('UP', token, condition, self.end if end is None else end,
                       2.5, .5, .5, '.01', key,
                       json.dumps({'order': {'salt': key, 'signature': 'offline'}}),
                       {}, {}, {}, 'FAK')
        p.reserve = 2.675
        if marker is not None:
            p.minimum_entry = marker
        return p

    def send(self, key, *, status=None, **kwargs):
        p = self.prepared(key, **kwargs)
        self.session.before_send(p, self.limits, self.kill)
        if status is not None:
            self.limits.pending_fills -= 1
            self.session.outcome(p, {'kind': status}, self.limits, self.kill)
            self.assertTrue(self.session.flush())
        return p

    def test_marker_is_in_the_existing_single_durable_send_record(self):
        with patch.object(self.session, '_durable_append', wraps=self.session._durable_append) as append:
            self.send('one')
        self.assertEqual(append.call_count, 1)
        durable = L.CanarySession.durable_view(self.path)
        self.assertIs(durable['intents']['one']['minimum_entry'], True)
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})

    def test_old_and_explicit_false_intents_do_not_consume_slots(self):
        self.send('old', marker=None)
        self.send('false', marker=False)
        self.assertEqual(self.session.minimum_entry_claims(), set())
        durable = L.CanarySession.durable_view(self.path)
        self.assertNotIn('minimum_entry', durable['intents']['old'])

    def test_no_match_survives_compaction_and_restart(self):
        self.send('miss', status=L.NO_MATCH)
        self.assertTrue(self.session.save(self.limits, self.kill))
        self.assertNotIn('miss', self.session.intents)
        self.session.close()
        self.boot()
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})

    def test_not_sent_after_durable_intent_still_consumes_the_slot(self):
        self.send('unsent', status=L.NOT_SENT)
        self.assertTrue(self.session.save(self.limits, self.kill))
        self.session.close()
        self.boot()
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})

    def test_pending_intent_survives_restart_and_remains_claimed(self):
        self.send('pending')
        self.session.close()
        self.boot()
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})

    def test_claims_are_per_token_market_and_expiry(self):
        self.send('up')
        self.send('down', token='down')
        self.send('other', condition='other', token='up')
        self.send('old', end=self.end - 500)
        self.assertEqual(self.session.minimum_entry_claims(), {
            ('condition', 'up', self.end), ('condition', 'down', self.end),
            ('other', 'up', self.end)})
        self.assertEqual(self.session.minimum_entry_claims(self.end), set())

    def test_archive_and_hot_state_are_combined(self):
        self.send('first', status=L.NO_MATCH)
        self.assertTrue(self.session.save(self.limits, self.kill))
        self.send('second', token='down')
        self.assertEqual(self.session.minimum_entry_claims(), {
            ('condition', 'up', self.end), ('condition', 'down', self.end)})

    def test_torn_archive_tail_is_removed_before_first_completed_append(self):
        self.send('miss', status=L.NO_MATCH)
        self.session.archive_path.write_bytes(b'{"key":')
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})
        self.assertTrue(self.session.save(self.limits, self.kill))
        records = [json.loads(line) for line in self.session.archive_path.read_bytes().splitlines()]
        self.assertEqual([r['key'] for r in records], ['miss'])
        self.session.close()
        self.boot()
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})

    def test_long_torn_tail_preserves_prior_completed_records(self):
        self.send('first', status=L.NO_MATCH)
        self.assertTrue(self.session.save(self.limits, self.kill))
        original = self.session.archive_path.read_bytes()
        with self.session.archive_path.open('ab') as f:
            f.write(b'{"unfinished":"' + b'x' * 20000)
        self.send('second', token='down', status=L.NO_MATCH)
        self.assertTrue(self.session.save(self.limits, self.kill))
        repaired = self.session.archive_path.read_bytes()
        self.assertTrue(repaired.startswith(original))
        self.assertEqual([json.loads(line)['key'] for line in repaired.splitlines()], ['first', 'second'])
        self.session.close()
        self.boot()
        self.assertEqual(self.session.minimum_entry_claims(), {
            ('condition', 'up', self.end), ('condition', 'down', self.end)})

    def test_complete_archive_record_without_newline_keeps_its_claim(self):
        self.send('first', status=L.NO_MATCH)
        self.assertTrue(self.session.save(self.limits, self.kill))
        archive = self.session.archive_path
        original = archive.read_bytes()
        archive.write_bytes(original[:-1])
        self.assertNotIn('first', self.session.intents)
        self.assertEqual(self.session.minimum_entry_claims(), {('condition', 'up', self.end)})
        self.send('second', token='down', status=L.NO_MATCH)
        self.assertTrue(self.session.save(self.limits, self.kill))
        self.assertTrue(archive.read_bytes().startswith(original))
        self.assertEqual([json.loads(line)['key'] for line in archive.read_bytes().splitlines()], ['first', 'second'])
        self.session.close()
        self.boot()
        self.assertEqual(self.session.minimum_entry_claims(), {
            ('condition', 'up', self.end), ('condition', 'down', self.end)})

    def test_corrupt_completed_archive_record_remains_rejected(self):
        self.session.archive_path.write_bytes(b'{"key":\n')
        with self.assertRaisesRegex(ValueError, 'damaged canary archive line'):
            self.session.minimum_entry_claims()

    def test_valid_json_with_invalid_archive_shape_is_not_discarded(self):
        self.send('miss', status=L.NO_MATCH)
        bad = b'{"key":"bad","intent":false}'
        self.session.archive_path.write_bytes(bad)
        with self.assertRaisesRegex(ValueError, 'damaged canary archive line'):
            self.session.minimum_entry_claims()
        self.assertFalse(self.session.save(self.limits, self.kill))
        self.assertIn('miss', self.session.intents)
        self.assertEqual(self.session.archive_path.read_bytes(), bad)

    def test_corrupt_marker_is_rejected_before_budget_or_intent_changes(self):
        p = self.prepared('invalid', marker='true')
        with self.assertRaisesRegex(L.Refused, 'minimum-entry marker'):
            self.session.before_send(p, self.limits, self.kill)
        self.assertEqual(self.limits.orders, 0)
        self.assertNotIn('invalid', self.session.intents)

    def test_corrupt_hot_or_archived_markers_fail_recovery(self):
        self.send('record', status=L.NO_MATCH)
        self.session.intents['record']['minimum_entry'] = 1
        with self.assertRaisesRegex(ValueError, 'minimum-entry marker'):
            self.session.minimum_entry_claims()
        self.session.intents['record']['minimum_entry'] = True
        self.assertTrue(self.session.save(self.limits, self.kill))
        archive = self.session.archive_path
        records = [json.loads(line) for line in archive.read_text().splitlines()]
        records[0]['intent']['minimum_entry'] = 'true'
        archive.write_text(''.join(json.dumps(row) + '\n' for row in records))
        with self.assertRaisesRegex(ValueError, 'minimum-entry marker'):
            self.session.minimum_entry_claims()

    def test_invalid_recovery_clock_is_rejected(self):
        for now in (True, -1, float('nan'), float('inf'), 'now'):
            with self.subTest(now=now), self.assertRaisesRegex(ValueError, 'recovery time'):
                self.session.minimum_entry_claims(now)


if __name__ == '__main__':
    unittest.main()
