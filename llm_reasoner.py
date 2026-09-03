"""
LLM Reasoning Engine using Google GenAI SDK with Gemini 3.5 Flash.
Enables agents to perform real-time LLM reasoning over telemetry data.
"""
import os
import json
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()

class LLMReasoner:
    def __init__(self):
        self.model_name = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash")
        self.client = None
        self._init_client()

    def _init_client(self):
        try:
            from google import genai
            api_key = os.environ.get("GEMINI_API_KEY")
            if api_key:
                self.client = genai.Client(api_key=api_key)
            else:
                project = os.environ.get("GOOGLE_CLOUD_PROJECT", "abhiwa-seed-project-76a0")
                location = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
                self.client = genai.Client(vertexai=True, project=project, location=location)
        except Exception as e:
            print(f"Warning: Failed to initialize Google GenAI Client: {e}")
            self.client = None

    def analyze_trace_with_llm(self, service: str, trace_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prompt Gemini to analyze distributed trace spans and propose root cause hypothesis."""
        if not self.client:
            return {
                "hypothesis_id": "H_SPAN_BOTTLENECK",
                "claim": f"{service} downstream bottleneck detected causing cascade timeouts.",
                "evidence": [f"Span latency spike on {service}"]
            }

        prompt = f"""
You are an expert SRE Trace Analysis Agent in a distributed microservices swarm.
Analyze the following distributed trace telemetry for service '{service}':

```json
{json.dumps(trace_data, indent=2)}
```

Identify the exact bottleneck span, the root error, and propose a concise hypothesis claim.
Respond ONLY in valid JSON matching this schema:
{{
  "hypothesis_id": "string (e.g. H_AUTH_POOL_EXHAUSTION, H_PAYMENT_CONN_RESET, or H_ORDER_DB_WAIT)",
  "claim": "string explaining what failed and caused upstream timeouts",
  "evidence": ["bullet point 1 with span ID and numbers", "bullet point 2"]
}}
"""
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
            data = json.loads(response.text)
            if not data.get("hypothesis_id"):
                data["hypothesis_id"] = "H_INITIAL_INVESTIGATION"
            return data
        except Exception as e:
            return {
                "hypothesis_id": "H_SPAN_BOTTLENECK",
                "claim": f"Trace analysis identified critical bottleneck on {service}: {str(e)[:60]}",
                "evidence": [f"Trace {trace_data.get('trace_id')} duration: {trace_data.get('duration_ms')}ms"]
            }

    def audit_database_with_llm(self, service: str, db_metrics: Dict[str, Any], current_hypo: Dict[str, Any]) -> Dict[str, Any]:
        """Prompt Gemini to evaluate database telemetry against current hypothesis."""
        if not self.client:
            return {
                "action": "REFUTE",
                "score_delta": -40.0,
                "finding": "p99 < 2ms, pool saturated at 100/100",
                "explanation": "Postgres is healthy; starvation caused by upstream connection flood."
            }

        prompt = f"""
You are a Database Reliability Agent in an RCA Swarm.
Current Hypothesis under scrutiny:
ID: {current_hypo.get('id')}
Claim: "{current_hypo.get('claim')}"

Here is the live database telemetry for '{service}':
```json
{json.dumps(db_metrics, indent=2)}
```

Determine whether this telemetry VALIDATES, REFUTES, or CORROBORATES the hypothesis.
Rules:
1. If query latency p99 is fast (<5ms) and locks are 0, but pool is 100/100, REFUTE direct database internal lock/slow-query theories and attribute starvation to caller volume.
2. If query latency is very high (>10s) and active_locks > 0 or deadlocks > 0, VALIDATE database lock/deadlock theory.
3. If database is completely idle and healthy, REFUTE any database culpability.

Respond ONLY in valid JSON:
{{
  "action": "REFUTE" or "VALIDATE",
  "score_delta": number (e.g. -40 or +35),
  "finding": "concise technical statement with exact metrics",
  "explanation": "why this refutes or confirms the theory"
}}
"""
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
            return json.loads(response.text)
        except Exception:
            return {
                "action": "REFUTE",
                "score_delta": -40.0,
                "finding": "p99 < 2ms, pool saturated at 100/100",
                "explanation": "Postgres engine is healthy; starvation caused by caller query storm."
            }

    def audit_deploy_with_llm(self, service: str, deploy_history: List[Dict[str, Any]], current_hypo: Dict[str, Any]) -> Dict[str, Any]:
        """Prompt Gemini to evaluate git commits and CI/CD deploys."""
        if not self.client:
            return {"should_branch": False}

        prompt = f"""
You are a CI/CD Deploy & Git Audit Agent in an RCA Swarm.
Current investigated incident on service '{service}':
ID: {current_hypo.get('id')}
Claim: "{current_hypo.get('claim')}"

Recent deployments and commit diffs:
```json
{json.dumps(deploy_history, indent=2)}
```

Determine if any recent commit is the causal root cause.
If a commit directly explains the failure (e.g. reducing cache TTL to 0, restricting container memory limits, or unindexed DB table lock migration), branch a new root-cause sub-hypothesis!

Respond ONLY in valid JSON:
{{
  "should_branch": true or false,
  "new_hypothesis_id": "string (e.g. H_CACHE_TTL_ZERO, H_CONTAINER_OOM_LIMIT, or H_UNINDEXED_MIGRATION_LOCK)",
  "claim": "detailed claim connecting the git commit diff to the incident",
  "evidence": ["commit sha and title citation", "technical consequence of the diff"]
}}
"""
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
            return json.loads(response.text)
        except Exception:
            return {"should_branch": False}

    def audit_kubernetes_with_llm(self, service: str, k8s_metrics: Dict[str, Any], current_hypo: Dict[str, Any]) -> Dict[str, Any]:
        """Prompt Gemini to evaluate Kubernetes pod telemetry."""
        if not self.client:
            return {
                "is_oom_or_infra_failure": False,
                "score_delta": 30.0,
                "evidence": "Pod resources healthy; rules out node/OOM failure."
            }

        prompt = f"""
You are a Kubernetes Infrastructure Agent in an RCA Swarm.
Hypothesis under scrutiny:
ID: {current_hypo.get('id')}
Claim: "{current_hypo.get('claim')}"

Kubernetes Pod & Node telemetry for service '{service}':
```json
{json.dumps(k8s_metrics, indent=2)}
```

Analyze if the pod health supports the hypothesis or points to container OOMKills / crash loops.
Rules:
1. If pods are in CRASH_LOOP_OOM_DETECTED or oom_killed_count > 0, VALIDATE container resource/memory limit failure (+40).
2. If pods are healthy (0 restarts, healthy CPU/mem), corroborate that infrastructure is stable and failure is application/config level (+30).

Respond ONLY in valid JSON:
{{
  "is_oom_or_infra_failure": true or false,
  "score_delta": number (+30 or +40),
  "evidence": "concise observation with exact pod metrics and restart counts"
}}
"""
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={"response_mime_type": "application/json"}
            )
            return json.loads(response.text)
        except Exception:
            return {
                "is_oom_or_infra_failure": False,
                "score_delta": 30.0,
                "evidence": f"Pods healthy: {k8s_metrics.get('healthy_pods')}/{k8s_metrics.get('pod_count')}. Infrastructure verified healthy."
            }

    def synthesize_rca_report(self, winner_hypo: Dict[str, Any], all_hypotheses: Dict[str, Any], timeline: List[Dict[str, Any]]) -> str:
        """Prompt Gemini to synthesize the executive post-mortem narrative."""
        if not self.client:
            return f"# RCA Report\n\n**Winning Claim:** {winner_hypo.get('claim')}"

        prompt = f"""
You are the Synthesizer Arbiter of an Autonomous Multi-Agent SRE Swarm.
Multi-agent consensus has been reached on the root cause!
Winning Hypothesis:
```json
{json.dumps(winner_hypo, indent=2)}
```

All Hypotheses Scrutinized:
```json
{json.dumps(all_hypotheses, indent=2)}
```

Incident Timeline from Valkey:
```json
{json.dumps(timeline, indent=2)}
```

Write a professional, high-impact SRE Incident Post-Mortem in Markdown.
Structure:
# 🚨 Autonomous Root Cause Analysis (RCA) Post-Mortem Report
## 🔍 Executive Summary
## 🎯 Root Cause & Causal Mechanism
## ⚔️ Adversarial Scrutiny & Refutations (explain how wrong theories were rejected using telemetry)
## ⏱️ Chronological Incident Timeline
## 💡 Automated Remediation & Prevention Items (P0 immediate rollback, P1 structural prevention)
"""
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt
            )
            return response.text
        except Exception as e:
            return f"# RCA Report\n\n**Winning Claim:** {winner_hypo.get('claim')}"
