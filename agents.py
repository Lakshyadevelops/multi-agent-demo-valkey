"""
Autonomous Multi-Agent RCA Swarm Actors with Real Gemini 3.5 Flash LLM Reasoning.
TraceExplorerAgent, DatabaseSleuthAgent, DeployScoutAgent, InfraK8sAgent, and SynthesizerArbiter
coordinate exclusively via Valkey Blackboard.
"""
import os
import json
import time
import asyncio
from typing import Dict, Any, Optional
from blackboard import Blackboard
from telemetry_mock import TelemetryMock
from llm_reasoner import LLMReasoner

class BaseAgent:
    def __init__(self, name: str, blackboard: Blackboard, llm: LLMReasoner):
        self.name = name
        self.bb = blackboard
        self.llm = llm
        self.running = True

    async def run(self):
        """Main agent loop: poll for events, pre-filter, process, and ACK."""
        while self.running:
            try:
                events = self.bb.read_events(consumer_name=self.name, count=3, block_ms=500)
                for msg_id, fields in events:
                    event_type = fields.get("type", "")
                    if self.pre_filter(event_type, fields):
                        await self.handle_event(msg_id, event_type, fields)
                    self.bb.ack_event(msg_id)
            except Exception as e:
                await asyncio.sleep(0.5)
            await asyncio.sleep(0.1)

    def pre_filter(self, event_type: str, fields: Dict[str, Any]) -> bool:
        return True

    async def handle_event(self, msg_id: str, event_type: str, fields: Dict[str, Any]):
        raise NotImplementedError

    def stop(self):
        self.running = False


class TraceExplorerAgent(BaseAgent):
    """
    Domain: Distributed traces, ingress spans, cascade latencies.
    Triggers on raw ingress alerts (e.g. 504 Gateway Timeout or 502 Bad Gateway).
    Isolates slow spans, invokes Gemini to deduce initial hypothesis, records to timeline.
    """
    def __init__(self, blackboard: Blackboard, llm: LLMReasoner):
        super().__init__("TraceExplorerAgent", blackboard, llm)

    def pre_filter(self, event_type: str, fields: Dict[str, Any]) -> bool:
        return event_type in ("ALERT_INGRESS_TIMEOUT", "ALERT_PAYMENT_FAILURE", "ALERT_LATENCY_SPIKE")

    async def handle_event(self, msg_id: str, event_type: str, fields: Dict[str, Any]):
        service = fields.get("service", "checkout-gw")
        scenario = fields.get("scenario", "scenario_cache_ttl")

        # Concurrency single-flight check
        if not self.bb.acquire_lock(service, "trace_inspection", self.name, ttl_seconds=15):
            return

        try:
            # 1. Fetch raw telemetry mock
            traces = TelemetryMock.get_trace_data(service, scenario=scenario)

            # 2. Real Gemini LLM Reasoning over distributed trace spans
            analysis = self.llm.analyze_trace_with_llm(service, traces)
            hypo_id = analysis.get("hypothesis_id", "H_INGRESS_TIMEOUT_INVESTIGATION")

            # Determine downstream target service implicated in traces
            target_service = service
            if traces.get("spans") and len(traces["spans"]) > 1:
                target_service = traces["spans"][1].get("service", service)

            # 3. Record anomaly to Valkey chronological timeline
            now = int(time.time() * 1000)
            self.bb.record_timeline_anomaly(
                now,
                service,
                f"Distributed trace {traces.get('trace_id')}: {service} failed. Gemini root cause hypothesis: {analysis.get('claim')}"
            )

            # 4. Create initial hypothesis in Valkey Hashes & Sorted Set
            hypo_data = {
                "id": hypo_id,
                "creator": self.name,
                "target_service": target_service,
                "scenario": scenario,
                "claim": analysis.get("claim", f"Bottleneck detected on {target_service}"),
                "supporting_evidence": analysis.get("evidence", [f"Latency spike: {traces.get('duration_ms')}ms"]),
                "contradictions": [],
                "status": "UNDER_SCRUTINY"
            }
            self.bb.create_hypothesis(hypo_data, initial_confidence=50.0)

            # 5. Emit stigmergic event to notify other specialists
            self.bb.emit_event("HYPOTHESIS_PROPOSED", {
                "id": hypo_id,
                "creator": self.name,
                "target_service": target_service,
                "scenario": scenario,
                "claim": hypo_data["claim"]
            })
        finally:
            self.bb.release_lock(service, "trace_inspection", self.name)


class DatabaseSleuthAgent(BaseAgent):
    """
    Domain: Postgres connection pools, row locks, slow queries.
    Uses Gemini LLM to audit DB telemetry and evaluate the active hypothesis.
    """
    def __init__(self, blackboard: Blackboard, llm: LLMReasoner):
        super().__init__("DatabaseSleuthAgent", blackboard, llm)

    def pre_filter(self, event_type: str, fields: Dict[str, Any]) -> bool:
        if event_type not in ("HYPOTHESIS_PROPOSED", "HYPOTHESIS_EVALUATED"):
            return False
        service = fields.get("target_service", "")
        return service in ("auth-api", "payment-service", "order-service", "postgres-db")

    async def handle_event(self, msg_id: str, event_type: str, fields: Dict[str, Any]):
        hypo_id = fields.get("id", "")
        service = fields.get("target_service", "auth-api")
        scenario = fields.get("scenario", "scenario_cache_ttl")
        if not hypo_id:
            return

        if not self.bb.acquire_lock(service, "db_telemetry_inspection", self.name, ttl_seconds=15):
            return

        try:
            current_hypo = self.bb.get_hypothesis(hypo_id)
            if not current_hypo:
                return

            # Fetch DB metrics
            db_metrics = TelemetryMock.get_database_metrics(service, scenario=scenario)

            # Real Gemini LLM Audit of database telemetry
            audit_result = self.llm.audit_database_with_llm(service, db_metrics, current_hypo)
            action = audit_result.get("action", "REFUTE")
            delta = float(audit_result.get("score_delta", -40.0))
            explanation = audit_result.get("explanation", "DB audit completed.")

            now = int(time.time() * 1000)
            if action == "REFUTE":
                new_score = self.bb.refute_hypothesis(
                    hypo_id=hypo_id,
                    agent_id=self.name,
                    contradiction=f"Gemini DB Audit: {explanation} ({audit_result.get('finding')})",
                    score_delta=delta
                )
                self.bb.record_timeline_anomaly(
                    now,
                    service,
                    f"DB Telemetry audited by Gemini: Refuted direct DB internal fault. Confidence docked to {new_score:.1f}."
                )
            else:
                new_score = self.bb.validate_hypothesis(
                    hypo_id=hypo_id,
                    agent_id=self.name,
                    evidence=f"Gemini DB Audit confirmed: {explanation}",
                    score_delta=delta
                )
                self.bb.record_timeline_anomaly(
                    now,
                    service,
                    f"DB Telemetry audited by Gemini: Confirmed database bottleneck (+{delta} -> {new_score:.1f})."
                )

            self.bb.emit_event("HYPOTHESIS_EVALUATED", {
                "id": hypo_id,
                "evaluator": self.name,
                "target_service": service,
                "scenario": scenario,
                "action": action
            })
        finally:
            self.bb.release_lock(service, "db_telemetry_inspection", self.name)


class DeployScoutAgent(BaseAgent):
    """
    Domain: Git commits, CI/CD deploys, feature flag flips.
    Audits commit history with Gemini; branches root-cause sub-hypothesis if causal commit identified.
    """
    def __init__(self, blackboard: Blackboard, llm: LLMReasoner):
        super().__init__("DeployScoutAgent", blackboard, llm)

    def pre_filter(self, event_type: str, fields: Dict[str, Any]) -> bool:
        return event_type in ("HYPOTHESIS_PROPOSED", "HYPOTHESIS_EVALUATED")

    async def handle_event(self, msg_id: str, event_type: str, fields: Dict[str, Any]):
        hypo_id = fields.get("id", "")
        service = fields.get("target_service", "")
        scenario = fields.get("scenario", "scenario_cache_ttl")
        if not service:
            return

        # Avoid looping on self-branched hypotheses
        if hypo_id in ("H_CACHE_TTL_ZERO", "H_CONTAINER_OOM_LIMIT", "H_UNINDEXED_MIGRATION_LOCK"):
            return

        if not self.bb.acquire_lock(service, "deploy_inspection", self.name, ttl_seconds=15):
            return

        try:
            current_hypo = self.bb.get_hypothesis(hypo_id) or {}
            deploys = TelemetryMock.get_deploy_history(service, scenario=scenario)
            if not deploys:
                return

            # Real Gemini LLM Audit of Git Diff
            branch_decision = self.llm.audit_deploy_with_llm(service, deploys, current_hypo)
            if branch_decision.get("should_branch"):
                new_id = branch_decision.get("new_hypothesis_id", f"H_DEPLOY_{service.upper()}")
                new_hypo_data = {
                    "id": new_id,
                    "creator": self.name,
                    "target_service": service,
                    "scenario": scenario,
                    "claim": branch_decision.get("claim", "Git deployment caused system failure"),
                    "supporting_evidence": branch_decision.get("evidence", [f"Deploy {deploys[0]['commit_sha']}"]),
                    "contradictions": [],
                    "status": "UNDER_SCRUTINY"
                }

                # Branch in Valkey
                self.bb.branch_hypothesis(parent_id=hypo_id, new_hypo=new_hypo_data, initial_confidence=45.0)
                # Boost confidence with git correlation
                self.bb.validate_hypothesis(
                    hypo_id=new_id,
                    agent_id=self.name,
                    evidence="Commit timestamp and diff causally precede the failure.",
                    score_delta=15.0
                )

                now = int(time.time() * 1000)
                self.bb.record_timeline_anomaly(
                    now,
                    service,
                    f"Gemini Git Audit branched {new_id}: {branch_decision.get('claim')[:60]}..."
                )
        finally:
            self.bb.release_lock(service, "deploy_inspection", self.name)


class InfraK8sAgent(BaseAgent):
    """
    Domain: Container pod restarts, OOMKills, kube-proxy, node CPU/Memory pressure.
    Audits pod health via Gemini; corroborates application vs infrastructure root causes.
    """
    def __init__(self, blackboard: Blackboard, llm: LLMReasoner):
        super().__init__("InfraK8sAgent", blackboard, llm)

    def pre_filter(self, event_type: str, fields: Dict[str, Any]) -> bool:
        if event_type == "HYPOTHESIS_PROPOSED":
            return True
        return False

    async def handle_event(self, msg_id: str, event_type: str, fields: Dict[str, Any]):
        hypo_id = fields.get("id", "")
        service = fields.get("target_service", "auth-api")
        scenario = fields.get("scenario", "scenario_cache_ttl")

        if not self.bb.acquire_lock(service, "k8s_infra_inspection", self.name, ttl_seconds=15):
            return

        try:
            current_hypo = self.bb.get_hypothesis(hypo_id) or {}
            k8s_metrics = TelemetryMock.get_kubernetes_metrics(service, scenario=scenario)

            # Real Gemini LLM Audit of Kubernetes telemetry
            infra_audit = self.llm.audit_kubernetes_with_llm(service, k8s_metrics, current_hypo)
            score_delta = float(infra_audit.get("score_delta", 30.0))
            evidence = infra_audit.get("evidence", "Kubernetes audit completed.")

            score = self.bb.validate_hypothesis(
                hypo_id=hypo_id,
                agent_id=self.name,
                evidence=f"Gemini K8s Audit: {evidence}",
                score_delta=score_delta
            )

            now = int(time.time() * 1000)
            self.bb.record_timeline_anomaly(
                now,
                service,
                f"K8s telemetry audited by Gemini. Corroborated {hypo_id} (+{score_delta:.0f} -> {score:.1f})."
            )
        finally:
            self.bb.release_lock(service, "k8s_infra_inspection", self.name)


class SynthesizerArbiter:
    """
    Role: Convergence sentinel and session supervisor.
    Polls ZREVRANGE rca:confidence 0 0 WITHSCORES.
    Once confidence >= 85 and contributors >= 2, prompts Gemini to synthesize final report.
    """
    def __init__(self, blackboard: Blackboard, llm: LLMReasoner, convergence_threshold: float = 85.0, min_contributors: int = 2, timeout_seconds: int = 45):
        self.bb = blackboard
        self.llm = llm
        self.convergence_threshold = convergence_threshold
        self.min_contributors = min_contributors
        self.timeout_seconds = timeout_seconds
        self.running = True

    async def monitor_and_arbitrate(self) -> Optional[str]:
        start_time = time.time()
        while self.running:
            elapsed = time.time() - start_time
            if elapsed > self.timeout_seconds:
                return self._finalize_session(reason="TIMEOUT_EXCEEDED")

            leaderboard = self.bb.get_leaderboard()
            if leaderboard:
                top_id, top_score = leaderboard[0]
                contributors_count = self.bb.get_contributors_count(top_id)

                if top_score >= self.convergence_threshold and contributors_count >= self.min_contributors:
                    return self._finalize_session(winner_id=top_id, score=top_score, reason="CONVERGENCE_REACHED")

            await asyncio.sleep(0.3)
        return None

    def _finalize_session(self, winner_id: Optional[str] = None, score: float = 0.0, reason: str = "CONVERGENCE_REACHED") -> str:
        timeline = self.bb.get_timeline()
        hypotheses = self.bb.get_all_hypotheses()
        winning_hypo = hypotheses.get(winner_id, {}) if winner_id else {}

        # Synthesize with real Gemini LLM
        final_verdict = self.llm.synthesize_rca_report(winning_hypo, hypotheses, timeline)

        # Store in Valkey Blackboard & Broadcast TERMINATE
        self.bb.store_final_verdict(final_verdict)
        self.bb.publish_control("TERMINATE")
        self.running = False
        return final_verdict
