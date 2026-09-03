"""
Redis/Valkey Blackboard implementation for Multi-Agent RCA Swarm.
Wraps Redis primitives: Streams, Sorted Sets, Hashes, Sets, Strings (Distributed Locks), Pub/Sub,
and an Agent Execution & Scratchpad Audit Log.
"""
import os
import json
import time
from typing import Dict, Any, List, Optional, Tuple
import redis

RELEASE_LOCK_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

class Blackboard:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, password: Optional[str] = None):
        self.host = host or os.environ.get("REDIS_HOST", "localhost")
        self.port = int(port or os.environ.get("REDIS_PORT", 6379))
        self.password = password or os.environ.get("REDIS_PASSWORD", None)
        
        self.client = redis.Redis(
            host=self.host,
            port=self.port,
            password=self.password,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        self._release_script = self.client.register_script(RELEASE_LOCK_LUA)

    def ping(self) -> bool:
        """Verify connectivity to Valkey/Redis instance."""
        return self.client.ping()

    def reset_blackboard(self):
        """Cleans all blackboard keys for a clean scenario run."""
        keys = [
            "stream:rca:events",
            "rca:timeline",
            "rca:hypotheses",
            "rca:confidence",
            "rca:final_verdict",
            "rca:agent:steps"
        ]
        dynamic_keys = self.client.keys("rca:hypothesis:*:contributors") + self.client.keys("lock:investigate:*")
        all_keys = keys + [k for k in dynamic_keys]
        if all_keys:
            self.client.delete(*all_keys)

    def init_stream(self, group_name: str = "agents-group", stream_name: str = "stream:rca:events"):
        """Initializes the Redis Stream and consumer group."""
        try:
            self.client.xgroup_create(stream_name, group_name, id="$", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def emit_event(self, event_type: str, payload: Dict[str, Any], stream_name: str = "stream:rca:events") -> str:
        """Publishes an event to the unified stigmergy event stream."""
        data = {
            "type": event_type,
            "timestamp": str(int(time.time() * 1000)),
            **{k: json.dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in payload.items()}
        }
        return self.client.xadd(stream_name, data)

    def read_events(
        self,
        consumer_name: str,
        group_name: str = "agents-group",
        stream_name: str = "stream:rca:events",
        count: int = 5,
        block_ms: int = 500,
        min_idle_ms: int = 3000
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Consumes tasks using XAUTOCLAIM for crash recovery and XREADGROUP for new events.
        """
        results: List[Tuple[str, Dict[str, Any]]] = []

        try:
            autoclaimed = self.client.xautoclaim(
                name=stream_name,
                groupname=group_name,
                consumername=consumer_name,
                min_idle_time=min_idle_ms,
                start_id="0-0",
                count=count
            )
            if len(autoclaimed) > 1 and autoclaimed[1]:
                for msg_id, fields in autoclaimed[1]:
                    results.append((msg_id, fields))
        except Exception:
            pass

        if results:
            return results

        response = self.client.xreadgroup(
            groupname=group_name,
            consumername=consumer_name,
            streams={stream_name: ">"},
            count=count,
            block=block_ms
        )

        if response:
            for _, messages in response:
                for msg_id, fields in messages:
                    results.append((msg_id, fields))

        return results

    def ack_event(self, message_id: str, group_name: str = "agents-group", stream_name: str = "stream:rca:events") -> int:
        """Acknowledge completed processing of a stream event."""
        return self.client.xack(stream_name, group_name, message_id)

    # --- Concurrency & Locks ---
    def acquire_lock(self, service: str, scope: str, worker_id: str, ttl_seconds: int = 10) -> bool:
        """Acquire a single-flight distributed lock with expiration."""
        lock_key = f"lock:investigate:{service}:{scope}"
        return bool(self.client.set(lock_key, worker_id, nx=True, ex=ttl_seconds))

    def release_lock(self, service: str, scope: str, worker_id: str) -> bool:
        """Safely release a distributed lock using Lua verification."""
        lock_key = f"lock:investigate:{service}:{scope}"
        return bool(self._release_script(keys=[lock_key], args=[worker_id]))

    # --- Timeline Operations ---
    def record_timeline_anomaly(self, timestamp_ms: int, service: str, anomaly: str):
        """Append an anomaly to the chronological timeline sorted set."""
        payload = json.dumps({"timestamp": timestamp_ms, "service": service, "anomaly": anomaly})
        self.client.zadd("rca:timeline", {payload: timestamp_ms})

    def get_timeline(self) -> List[Dict[str, Any]]:
        """Retrieve chronological microservice anomalies."""
        items = self.client.zrange("rca:timeline", 0, -1, withscores=True)
        timeline = []
        for raw_json, score in items:
            try:
                data = json.loads(raw_json)
                timeline.append(data)
            except Exception:
                timeline.append({"timestamp": score, "raw": raw_json})
        return timeline

    # --- Hypothesis Operations ---
    def create_hypothesis(self, hypo: Dict[str, Any], initial_confidence: float) -> str:
        """Store a new hypothesis in rca:hypotheses and seed confidence."""
        hypo_id = hypo["id"]
        creator = hypo["creator"]
        self.client.hset("rca:hypotheses", hypo_id, json.dumps(hypo))
        self.client.zadd("rca:confidence", {hypo_id: initial_confidence})
        self.client.sadd(f"rca:hypothesis:{hypo_id}:contributors", creator)
        return hypo_id

    def get_hypothesis(self, hypo_id: str) -> Optional[Dict[str, Any]]:
        raw = self.client.hget("rca:hypotheses", hypo_id)
        if raw:
            return json.loads(raw)
        return None

    def get_all_hypotheses(self) -> Dict[str, Dict[str, Any]]:
        raw_dict = self.client.hgetall("rca:hypotheses")
        return {hid: json.loads(val) for hid, val in raw_dict.items()}

    def validate_hypothesis(self, hypo_id: str, agent_id: str, evidence: str, score_delta: float = 30.0) -> float:
        hypo = self.get_hypothesis(hypo_id)
        if not hypo:
            raise ValueError(f"Hypothesis {hypo_id} not found")

        if evidence not in hypo["supporting_evidence"]:
            hypo["supporting_evidence"].append(evidence)

        new_score = self.client.zincrby("rca:confidence", score_delta, hypo_id)
        if new_score >= 80:
            hypo["status"] = "VALIDATED"
        self.client.hset("rca:hypotheses", hypo_id, json.dumps(hypo))
        self.client.sadd(f"rca:hypothesis:{hypo_id}:contributors", agent_id)
        return new_score

    def refute_hypothesis(self, hypo_id: str, agent_id: str, contradiction: str, score_delta: float = -40.0) -> float:
        hypo = self.get_hypothesis(hypo_id)
        if not hypo:
            raise ValueError(f"Hypothesis {hypo_id} not found")

        if contradiction not in hypo["contradictions"]:
            hypo["contradictions"].append(contradiction)

        new_score = self.client.zincrby("rca:confidence", score_delta, hypo_id)
        if new_score <= 0:
            hypo["status"] = "REFUTED"
        self.client.hset("rca:hypotheses", hypo_id, json.dumps(hypo))
        self.client.sadd(f"rca:hypothesis:{hypo_id}:contributors", agent_id)
        return new_score

    def branch_hypothesis(
        self,
        parent_id: str,
        new_hypo: Dict[str, Any],
        initial_confidence: float = 35.0
    ) -> str:
        new_id = new_hypo["id"]
        self.create_hypothesis(new_hypo, initial_confidence)
        self.emit_event("HYPOTHESIS_PROPOSED", {
            "id": new_id,
            "parent_id": parent_id,
            "creator": new_hypo["creator"],
            "target_service": new_hypo["target_service"],
            "scenario": new_hypo.get("scenario", ""),
            "claim": new_hypo["claim"]
        })
        return new_id

    def get_leaderboard(self) -> List[Tuple[str, float]]:
        return self.client.zrevrange("rca:confidence", 0, -1, withscores=True)

    def get_contributors_count(self, hypo_id: str) -> int:
        return self.client.scard(f"rca:hypothesis:{hypo_id}:contributors")

    def get_contributors(self, hypo_id: str) -> List[str]:
        return list(self.client.smembers(f"rca:hypothesis:{hypo_id}:contributors"))

    # --- Agent Execution Lifecycle & Scratchpad Flow ---
    def record_agent_step(
        self,
        agent: str,
        trigger_event: str,
        phase: str,
        telemetry_inspected: str,
        reasoning: str,
        blackboard_mutation: str,
        timestamp_ms: Optional[int] = None
    ):
        """
        Records an execution step and scratchpad entry into rca:agent:steps.
        Enables real-time visualization of who triggered when, what was read, and what was written.
        """
        now = timestamp_ms or int(time.time() * 1000)
        entry = {
            "timestamp": now,
            "agent": agent,
            "trigger_event": trigger_event,
            "phase": phase,
            "telemetry_inspected": telemetry_inspected,
            "reasoning": reasoning,
            "blackboard_mutation": blackboard_mutation
        }
        self.client.rpush("rca:agent:steps", json.dumps(entry))

    def get_agent_steps(self) -> List[Dict[str, Any]]:
        """Fetch all recorded agent execution and scratchpad steps in chronological order."""
        raw_list = self.client.lrange("rca:agent:steps", 0, -1)
        steps = []
        for raw in raw_list:
            try:
                steps.append(json.loads(raw))
            except Exception:
                pass
        return steps

    # --- Control & Final Verdict ---
    def publish_control(self, command: str) -> int:
        return self.client.publish("rca:control:broadcast", command)

    def get_pubsub_listener(self):
        pubsub = self.client.pubsub()
        pubsub.subscribe("rca:control:broadcast")
        return pubsub

    def store_final_verdict(self, verdict_md: str):
        self.client.set("rca:final_verdict", verdict_md)

    def get_final_verdict(self) -> Optional[str]:
        return self.client.get("rca:final_verdict")
