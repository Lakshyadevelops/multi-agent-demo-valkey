"""
Unit tests for Redis/Valkey Blackboard primitives and multi-agent coordination.
"""
import unittest
import time
from blackboard import Blackboard

class TestBlackboard(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bb = Blackboard()
        try:
            cls.bb.ping()
        except Exception:
            raise unittest.SkipTest("Redis/Valkey not reachable on localhost:6379")

    def setUp(self):
        self.bb.reset_blackboard()

    def test_distributed_locking_single_flight(self):
        service = "auth-api"
        scope = "trace_inspection"
        
        # Worker 1 acquires lock
        acquired = self.bb.acquire_lock(service, scope, "Worker1", ttl_seconds=5)
        self.assertTrue(acquired)

        # Worker 2 attempts to acquire same lock - should fail (single flight)
        acquired_2 = self.bb.acquire_lock(service, scope, "Worker2", ttl_seconds=5)
        self.assertFalse(acquired_2)

        # Worker 2 attempts to release Worker 1's lock - should fail (Lua token check)
        released_fake = self.bb.release_lock(service, scope, "Worker2")
        self.assertFalse(released_fake)

        # Worker 1 releases lock cleanly
        released = self.bb.release_lock(service, scope, "Worker1")
        self.assertTrue(released)

        # Worker 2 can now acquire
        acquired_now = self.bb.acquire_lock(service, scope, "Worker2", ttl_seconds=5)
        self.assertTrue(acquired_now)
        self.bb.release_lock(service, scope, "Worker2")

    def test_hypothesis_adversarial_evaluation_and_branching(self):
        # 1. Propose H1
        h1 = {
            "id": "H1",
            "creator": "TraceExplorer",
            "target_service": "auth-api",
            "claim": "Direct DB connection lock",
            "supporting_evidence": ["High latency span"],
            "contradictions": [],
            "status": "UNDER_SCRUTINY"
        }
        self.bb.create_hypothesis(h1, initial_confidence=50.0)
        self.assertEqual(self.bb.get_contributors_count("H1"), 1)

        # 2. Refute H1
        new_score = self.bb.refute_hypothesis("H1", "DatabaseSleuth", "DB p99 < 2ms, no locks found", score_delta=-60.0)
        self.assertEqual(new_score, -10.0)
        updated_h1 = self.bb.get_hypothesis("H1")
        self.assertEqual(updated_h1["status"], "REFUTED")
        self.assertEqual(self.bb.get_contributors_count("H1"), 2)

        # 3. Branch H2
        h2 = {
            "id": "H2",
            "creator": "DeployScout",
            "target_service": "auth-api",
            "claim": "Cache TTL set to 0",
            "supporting_evidence": ["Commit diff found"],
            "contradictions": [],
            "status": "UNDER_SCRUTINY"
        }
        self.bb.branch_hypothesis("H1", h2, initial_confidence=40.0)
        self.assertEqual(self.bb.get_contributors_count("H2"), 1)

        # 4. Validate H2 with corroboration
        val_score = self.bb.validate_hypothesis("H2", "InfraK8s", "Pod resources healthy, confirms app config error", score_delta=50.0)
        self.assertEqual(val_score, 90.0)
        self.assertEqual(self.bb.get_contributors_count("H2"), 2)

        # 5. Leaderboard inspection
        leaderboard = self.bb.get_leaderboard()
        self.assertEqual(leaderboard[0][0], "H2")
        self.assertEqual(leaderboard[0][1], 90.0)
        self.assertEqual(leaderboard[1][0], "H1")
        self.assertEqual(leaderboard[1][1], -10.0)

    def test_stream_stigmergic_handoff(self):
        self.bb.init_stream(group_name="test-group", stream_name="stream:rca:test")
        
        # Emit event
        msg_id = self.bb.emit_event("ALERT", {"code": 504}, stream_name="stream:rca:test")
        self.assertIsNotNone(msg_id)

        # Read event via consumer group
        events = self.bb.read_events(
            consumer_name="test-agent",
            group_name="test-group",
            stream_name="stream:rca:test",
            count=1,
            block_ms=500
        )
        self.assertEqual(len(events), 1)
        rec_id, fields = events[0]
        self.assertEqual(fields["type"], "ALERT")
        self.assertEqual(fields["code"], "504")

        # Ack event
        acked = self.bb.ack_event(rec_id, group_name="test-group", stream_name="stream:rca:test")
        self.assertEqual(acked, 1)

if __name__ == "__main__":
    unittest.main()
