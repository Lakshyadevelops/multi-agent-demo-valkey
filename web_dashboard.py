"""
Interactive Educational Web Dashboard for Autonomous Multi-Agent RCA Swarm.
Visualizes Valkey Blackboard primitives, Stigmergic Event Streams,
and the complete step-by-step Agent Trigger & Scratchpad Activity Flow.
"""
import os
import json
import time
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from urllib.parse import urlparse
from blackboard import Blackboard
from telemetry_mock import TelemetryMock
from llm_reasoner import LLMReasoner

bb = Blackboard()
llm = LLMReasoner()

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Autonomous Multi-Agent RCA Swarm | Valkey Blackboard Demo</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
    <style>
        .pulse-border { animation: pulseBorder 2s infinite; }
        @keyframes pulseBorder { 0% { border-color: rgba(59, 130, 246, 0.4); } 50% { border-color: rgba(59, 130, 246, 1); } 100% { border-color: rgba(59, 130, 246, 0.4); } }
        .step-line::before {
            content: '';
            position: absolute;
            top: 2rem;
            bottom: -1rem;
            left: 1.25rem;
            width: 2px;
            background: #334155;
            z-index: 0;
        }
        .step-item:last-child .step-line::before {
            display: none;
        }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 font-sans min-h-screen">
    <!-- Navbar -->
    <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur px-6 py-4 flex flex-wrap items-center justify-between sticky top-0 z-50">
        <div class="flex items-center space-x-3">
            <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center text-white text-xl shadow-lg shadow-cyan-500/20">
                <i class="fa-solid fa-brain"></i>
            </div>
            <div>
                <h1 class="text-xl font-bold text-white tracking-wide">Multi-Agent RCA Swarm</h1>
                <p class="text-xs text-cyan-400 font-mono">Shared In-Memory Blackboard powered by Valkey</p>
            </div>
        </div>
        <div class="flex items-center space-x-4 mt-2 sm:mt-0">
            <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-emerald-950/80 text-emerald-400 border border-emerald-800">
                <span class="w-2 h-2 mr-2 bg-emerald-400 rounded-full animate-ping"></span>
                Valkey 9.1 Connected
            </span>
            <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-purple-950/80 text-purple-300 border border-purple-800">
                <i class="fa-solid fa-robot mr-1.5"></i> Gemini 3.5 Flash Active
            </span>
        </div>
    </header>

    <main class="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        <!-- Control Panel & Scenario Selection -->
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl">
            <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                    <h2 class="text-lg font-semibold text-white flex items-center">
                        <i class="fa-solid fa-sliders text-cyan-400 mr-2"></i> Interactive Scenario Selector
                    </h2>
                    <p class="text-xs text-slate-400">Choose an incident scenario to simulate and watch the swarm converge stigmergically.</p>
                </div>
                <div class="flex flex-wrap items-center gap-3">
                    <select id="scenarioSelect" class="bg-slate-950 border border-slate-700 text-slate-200 text-sm rounded-xl px-4 py-2.5 focus:ring-2 focus:ring-cyan-500 focus:outline-none">
                        <option value="scenario_cache_ttl">Scenario 1: Redis Session Cache TTL Collapse (Ingress 504 -> DB Pool Full)</option>
                        <option value="scenario_k8s_oom">Scenario 2: Payment Container OOMKill Cascade (HTTP 502 -> Pod Restarts)</option>
                        <option value="scenario_db_deadlock">Scenario 3: Unindexed Migration Table Lock (Order 500 -> Exclusive Lock)</option>
                    </select>
                    <button onclick="triggerIncident()" id="triggerBtn" class="bg-gradient-to-r from-red-600 to-rose-600 hover:from-red-500 hover:to-rose-500 text-white font-semibold text-sm px-5 py-2.5 rounded-xl shadow-lg shadow-rose-600/30 transition-all flex items-center gap-2">
                        <i class="fa-solid fa-bolt"></i> Inject Incident Alert
                    </button>
                    <button onclick="resetBlackboard()" class="bg-slate-800 hover:bg-slate-700 text-slate-300 text-sm px-4 py-2.5 rounded-xl border border-slate-700 transition flex items-center gap-2">
                        <i class="fa-solid fa-rotate-left"></i> Reset
                    </button>
                </div>
            </div>
        </div>

        <!-- Educational Callout: Why Valkey Solves Multi-Agent Coordination Gaps -->
        <div class="bg-gradient-to-r from-blue-950/40 via-slate-900 to-cyan-950/40 border border-cyan-800/40 rounded-2xl p-6 relative overflow-hidden">
            <div class="flex items-center gap-3 mb-4">
                <div class="p-2 bg-cyan-500/10 rounded-lg text-cyan-400">
                    <i class="fa-solid fa-graduation-cap text-xl"></i>
                </div>
                <div>
                    <h3 class="text-base font-bold text-white">Why Valkey Solves Critical Gaps in Multi-Agent Systems</h3>
                    <p class="text-xs text-slate-400">Key architectural advantages over naive prompt-chaining and monolithic orchestrators</p>
                </div>
            </div>
            <div class="grid grid-cols-1 md:grid-cols-4 gap-4 text-xs">
                <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-4">
                    <div class="text-cyan-400 font-bold flex items-center gap-2 mb-1">
                        <i class="fa-solid fa-database"></i> 1. Token Explosion
                    </div>
                    <p class="text-slate-400">Instead of passing megabytes of trace JSONs inside LLM prompts across turns, agents store data in <strong>Valkey Hashes</strong> and pass lightweight 20-byte IDs.</p>
                </div>
                <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-4">
                    <div class="text-emerald-400 font-bold flex items-center gap-2 mb-1">
                        <i class="fa-solid fa-lock"></i> 2. Race Conditions
                    </div>
                    <p class="text-slate-400">Atomic single-flight locks (<code>SET NX EX</code>) ensure only one agent inspects a telemetry provider at once, eliminating redundant API costs.</p>
                </div>
                <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-4">
                    <div class="text-amber-400 font-bold flex items-center gap-2 mb-1">
                        <i class="fa-solid fa-network-wired"></i> 3. Stigmergic Streams
                    </div>
                    <p class="text-slate-400">No central orchestrator bottleneck. <strong>Valkey Streams</strong> (<code>XREADGROUP</code>) allow autonomous workers to react directly to blackboard state changes.</p>
                </div>
                <div class="bg-slate-900/90 border border-slate-800 rounded-xl p-4">
                    <div class="text-purple-400 font-bold flex items-center gap-2 mb-1">
                        <i class="fa-solid fa-users"></i> 4. Echo-Chamber Defense
                    </div>
                    <p class="text-slate-400"><strong>Valkey Sets</strong> track unique contributors (<code>SCARD >= 2</code>). A single agent cannot self-validate to artificially inflate confidence.</p>
                </div>
            </div>
        </div>

        <!-- NEW VISUALIZATION SECTION: Agent Trigger & Scratchpad Activity Flow -->
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 shadow-xl space-y-4">
            <div class="flex items-center justify-between border-b border-slate-800 pb-3">
                <div>
                    <h3 class="text-base font-bold text-white flex items-center gap-2">
                        <i class="fa-solid fa-route text-cyan-400"></i> Agent Trigger Timeline & Shared Scratchpad Activity
                    </h3>
                    <p class="text-xs text-slate-400">Step-by-step trace of which agent woke up, what telemetry it inspected, its Gemini reasoning, and what it mutated on the Valkey Blackboard.</p>
                </div>
                <span class="text-xs font-mono px-3 py-1 bg-slate-800 rounded-full text-slate-300">rca:agent:steps (Valkey List)</span>
            </div>
            
            <div id="agentStepsContainer" class="space-y-4 relative">
                <div class="text-xs text-slate-500 italic text-center py-8">Awaiting swarm activation... Trigger an incident to view live agent triggers and scratchpad writes.</div>
            </div>
        </div>

        <!-- Live Blackboard State Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <!-- Col 1: Leaderboard & Dynamic Confidence -->
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <div class="flex items-center justify-between">
                    <h3 class="font-bold text-white text-sm flex items-center gap-2">
                        <i class="fa-solid fa-trophy text-amber-400"></i> Hypothesis Leaderboard
                    </h3>
                    <span class="text-xs font-mono text-cyan-400">rca:confidence</span>
                </div>
                <div id="leaderboardList" class="space-y-3">
                    <div class="text-xs text-slate-500 italic text-center py-6">Waiting for incident trigger...</div>
                </div>
            </div>

            <!-- Col 2: Active Hypotheses Cards -->
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4 lg:col-span-2">
                <div class="flex items-center justify-between">
                    <h3 class="font-bold text-white text-sm flex items-center gap-2">
                        <i class="fa-solid fa-layer-group text-blue-400"></i> Structured Hypotheses Store
                    </h3>
                    <span class="text-xs font-mono text-cyan-400">rca:hypotheses (Hashes)</span>
                </div>
                <div id="hypothesesList" class="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <div class="text-xs text-slate-500 italic col-span-2 text-center py-6">No hypotheses proposed yet.</div>
                </div>
            </div>
        </div>

        <!-- Lower Grid: Timeline & Streams Activity -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <!-- Timeline -->
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <div class="flex items-center justify-between">
                    <h3 class="font-bold text-white text-sm flex items-center gap-2">
                        <i class="fa-solid fa-timeline text-purple-400"></i> Chronological Incident Timeline
                    </h3>
                    <span class="text-xs font-mono text-cyan-400">rca:timeline (Sorted Set)</span>
                </div>
                <div id="timelineContainer" class="space-y-2 max-h-72 overflow-y-auto pr-1">
                    <div class="text-xs text-slate-500 italic text-center py-6">Timeline is clear.</div>
                </div>
            </div>

            <!-- Valkey Event Stream Log -->
            <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
                <div class="flex items-center justify-between">
                    <h3 class="font-bold text-white text-sm flex items-center gap-2">
                        <i class="fa-solid fa-satellite-dish text-emerald-400"></i> Stigmergy Event Stream
                    </h3>
                    <span class="text-xs font-mono text-cyan-400">stream:rca:events (Streams)</span>
                </div>
                <div id="streamLog" class="space-y-2 max-h-72 overflow-y-auto font-mono text-xs pr-1">
                    <div class="text-xs text-slate-500 italic text-center py-6 font-sans">Awaiting stream events...</div>
                </div>
            </div>
        </div>

        <!-- Final Synthesized Post-Mortem Report -->
        <div id="verdictSection" class="hidden bg-slate-900 border border-emerald-600/40 rounded-2xl p-6 shadow-2xl relative">
            <div class="flex items-center justify-between mb-4 border-b border-slate-800 pb-3">
                <h3 class="text-lg font-bold text-emerald-400 flex items-center gap-2">
                    <i class="fa-solid fa-flag-checkered"></i> Autonomous Post-Mortem Report (Gemini Synthesized)
                </h3>
                <span class="text-xs font-mono px-3 py-1 bg-emerald-950 border border-emerald-800 text-emerald-300 rounded-full">
                    Consensus Reached & Verified in Valkey
                </span>
            </div>
            <div id="verdictContent" class="prose prose-invert max-w-none text-sm text-slate-200 leading-relaxed space-y-3">
            </div>
        </div>
    </main>

    <script>
        const AGENT_COLORS = {
            'TraceExplorerAgent': 'from-blue-600 to-cyan-600 text-cyan-300 border-cyan-800',
            'DatabaseSleuthAgent': 'from-emerald-600 to-teal-600 text-emerald-300 border-emerald-800',
            'DeployScoutAgent': 'from-amber-600 to-orange-600 text-amber-300 border-amber-800',
            'InfraK8sAgent': 'from-purple-600 to-indigo-600 text-purple-300 border-purple-800',
            'SynthesizerArbiter': 'from-rose-600 to-pink-600 text-rose-300 border-rose-800'
        };

        const AGENT_ICONS = {
            'TraceExplorerAgent': 'fa-magnifying-glass-chart',
            'DatabaseSleuthAgent': 'fa-database',
            'DeployScoutAgent': 'fa-code-branch',
            'InfraK8sAgent': 'fa-cubes',
            'SynthesizerArbiter': 'fa-gavel'
        };

        async function fetchState() {
            try {
                const res = await fetch('/api/state');
                const data = await res.json();
                renderAgentSteps(data.agent_steps);
                renderLeaderboard(data.leaderboard, data.hypotheses);
                renderHypotheses(data.hypotheses, data.contributors);
                renderTimeline(data.timeline);
                renderStream(data.stream_events);
                renderVerdict(data.verdict);
            } catch(e) {
                console.error(e);
            }
        }

        function renderAgentSteps(steps) {
            const container = document.getElementById('agentStepsContainer');
            if (!steps || steps.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 italic text-center py-8">Awaiting swarm activation... Trigger an incident to view live agent triggers and scratchpad writes.</div>';
                return;
            }

            container.innerHTML = steps.map((step, idx) => {
                const color = AGENT_COLORS[step.agent] || 'from-slate-600 to-slate-700 text-slate-300 border-slate-800';
                const icon = AGENT_ICONS[step.agent] || 'fa-user-gear';
                const ts = new Date(step.timestamp).toLocaleTimeString();

                return `
                <div class="step-item relative flex items-start gap-4">
                    <div class="step-line flex-shrink-0 z-10">
                        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr ${color} flex items-center justify-center text-white shadow-md text-sm">
                            <i class="fa-solid ${icon}"></i>
                        </div>
                    </div>
                    <div class="flex-grow bg-slate-950 border border-slate-800 rounded-xl p-4 shadow-sm space-y-2">
                        <div class="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800/80 pb-2">
                            <div class="flex items-center gap-2">
                                <span class="font-bold text-sm text-white">${step.agent}</span>
                                <span class="text-[10px] uppercase tracking-wider px-2 py-0.5 rounded bg-slate-800 text-cyan-300 font-mono font-bold">${step.phase}</span>
                            </div>
                            <div class="flex items-center gap-3 text-xs text-slate-400">
                                <span>Trigger: <code class="text-emerald-400 text-[11px] font-mono">${step.trigger_event}</code></span>
                                <span class="font-mono text-[10px] text-slate-500">${ts}</span>
                            </div>
                        </div>

                        <div class="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs pt-1">
                            <div class="bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                                <span class="text-[10px] font-bold text-slate-400 uppercase tracking-wider block mb-1">
                                    <i class="fa-solid fa-eye text-cyan-400 mr-1"></i> Telemetry Inspected
                                </span>
                                <p class="text-slate-300 font-mono text-[11px] leading-relaxed">${step.telemetry_inspected}</p>
                            </div>
                            <div class="bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                                <span class="text-[10px] font-bold text-purple-400 uppercase tracking-wider block mb-1">
                                    <i class="fa-solid fa-brain text-purple-400 mr-1"></i> Gemini 3.5 Flash Reasoning
                                </span>
                                <p class="text-slate-200 text-[11px] leading-relaxed">${step.reasoning}</p>
                            </div>
                            <div class="bg-slate-900/80 p-2.5 rounded-lg border border-slate-800">
                                <span class="text-[10px] font-bold text-emerald-400 uppercase tracking-wider block mb-1">
                                    <i class="fa-solid fa-pen-to-square text-emerald-400 mr-1"></i> Blackboard Scratchpad Mutation
                                </span>
                                <p class="text-emerald-300 font-mono text-[11px] leading-relaxed">${step.blackboard_mutation}</p>
                            </div>
                        </div>
                    </div>
                </div>`;
            }).join('');
        }

        function renderLeaderboard(leaderboard, hypotheses) {
            const container = document.getElementById('leaderboardList');
            if (!leaderboard || leaderboard.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 italic text-center py-6">Waiting for incident trigger...</div>';
                return;
            }
            container.innerHTML = leaderboard.map(([hid, score], idx) => {
                const hdata = hypotheses[hid] || {};
                const status = hdata.status || 'UNDER_SCRUTINY';
                const pct = Math.max(0, Math.min(100, score));
                let colorClass = 'bg-amber-500';
                if (score >= 80) colorClass = 'bg-emerald-500';
                if (score <= 0) colorClass = 'bg-rose-500';

                return `
                <div class="bg-slate-950 p-3 rounded-xl border border-slate-800">
                    <div class="flex justify-between items-center text-xs mb-1">
                        <span class="font-bold text-white font-mono">#${idx+1} ${hid}</span>
                        <span class="font-bold ${score >= 80 ? 'text-emerald-400' : (score <= 0 ? 'text-rose-400' : 'text-amber-400')}">${score.toFixed(1)} pts</span>
                    </div>
                    <div class="w-full bg-slate-800 h-2 rounded-full overflow-hidden mb-1">
                        <div class="${colorClass} h-full transition-all duration-500" style="width: ${pct}%"></div>
                    </div>
                    <div class="flex justify-between text-[10px] text-slate-400">
                        <span>Status: <strong class="text-slate-200">${status}</strong></span>
                        <span>Target: <strong class="text-cyan-400">${hdata.target_service || 'unknown'}</strong></span>
                    </div>
                </div>`;
            }).join('');
        }

        function renderHypotheses(hypotheses, contributorsMap) {
            const container = document.getElementById('hypothesesList');
            const keys = Object.keys(hypotheses || {});
            if (keys.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 italic col-span-2 text-center py-6">No hypotheses proposed yet.</div>';
                return;
            }
            container.innerHTML = keys.map(hid => {
                const h = hypotheses[hid];
                const contribs = (contributorsMap && contributorsMap[hid]) || [];
                const statusColor = h.status === 'VALIDATED' ? 'bg-emerald-950 text-emerald-300 border-emerald-800' : (h.status === 'REFUTED' ? 'bg-rose-950 text-rose-300 border-rose-800' : 'bg-amber-950 text-amber-300 border-amber-800');

                return `
                <div class="bg-slate-950 p-4 rounded-xl border border-slate-800 flex flex-col justify-between">
                    <div>
                        <div class="flex items-center justify-between mb-2">
                            <span class="font-mono text-xs font-bold text-cyan-300">${h.id}</span>
                            <span class="text-[10px] uppercase font-bold px-2 py-0.5 rounded border ${statusColor}">${h.status}</span>
                        </div>
                        <p class="text-xs text-slate-200 font-medium mb-3">${h.claim}</p>
                        
                        ${h.supporting_evidence && h.supporting_evidence.length ? `
                        <div class="mb-2">
                            <span class="text-[10px] font-bold text-emerald-400 block mb-1">Evidence:</span>
                            <ul class="text-[11px] text-slate-300 space-y-1 list-disc pl-3">
                                ${h.supporting_evidence.map(e => `<li>${e}</li>`).join('')}
                            </ul>
                        </div>` : ''}

                        ${h.contradictions && h.contradictions.length ? `
                        <div class="mb-2">
                            <span class="text-[10px] font-bold text-rose-400 block mb-1">Contradictions (Refutations):</span>
                            <ul class="text-[11px] text-slate-300 space-y-1 list-disc pl-3">
                                ${h.contradictions.map(c => `<li>${c}</li>`).join('')}
                            </ul>
                        </div>` : ''}
                    </div>

                    <div class="mt-3 pt-2 border-t border-slate-800/80 flex items-center justify-between text-[10px] text-slate-400">
                        <span>Creator: <strong class="text-slate-200">${h.creator}</strong></span>
                        <div class="flex items-center gap-1">
                            <i class="fa-solid fa-users text-cyan-400"></i>
                            <span>${contribs.length} contributors</span>
                        </div>
                    </div>
                </div>`;
            }).join('');
        }

        function renderTimeline(timeline) {
            const container = document.getElementById('timelineContainer');
            if (!timeline || timeline.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 italic text-center py-6">Timeline is clear.</div>';
                return;
            }
            container.innerHTML = timeline.map(item => {
                const ts = new Date(item.timestamp).toLocaleTimeString();
                return `
                <div class="bg-slate-950 p-2.5 rounded-lg border border-slate-800 text-xs flex gap-3 items-start">
                    <span class="text-slate-500 font-mono text-[10px] whitespace-nowrap mt-0.5">${ts}</span>
                    <div>
                        <span class="font-bold text-cyan-400 mr-1.5">[${item.service}]</span>
                        <span class="text-slate-300">${item.anomaly}</span>
                    </div>
                </div>`;
            }).join('');
        }

        function renderStream(streamEvents) {
            const container = document.getElementById('streamLog');
            if (!streamEvents || streamEvents.length === 0) {
                container.innerHTML = '<div class="text-xs text-slate-500 italic text-center py-6 font-sans">Awaiting stream events...</div>';
                return;
            }
            container.innerHTML = streamEvents.map(evt => {
                return `
                <div class="bg-slate-950 p-2 rounded border border-slate-800/80 text-[11px]">
                    <div class="flex justify-between text-slate-400 text-[10px] mb-0.5">
                        <span class="text-emerald-400 font-bold">${evt.type}</span>
                        <span class="text-slate-500">${evt.id}</span>
                    </div>
                    <div class="text-slate-300 text-[10px] truncate">${JSON.stringify(evt.payload)}</div>
                </div>`;
            }).join('');
        }

        function renderVerdict(verdict) {
            const section = document.getElementById('verdictSection');
            const content = document.getElementById('verdictContent');
            if (verdict && verdict.trim().length > 0) {
                section.classList.remove('hidden');
                content.innerHTML = marked.parse(verdict);
            } else {
                section.classList.add('hidden');
            }
        }

        async function triggerIncident() {
            const scenario = document.getElementById('scenarioSelect').value;
            const btn = document.getElementById('triggerBtn');
            btn.disabled = true;
            btn.innerHTML = '<i class="fa-solid fa-spinner animate-spin"></i> Swarm Reasoning in Progress...';
            try {
                await fetch('/api/trigger', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({scenario})
                });
            } catch(e) {
                console.error(e);
            } finally {
                setTimeout(() => {
                    btn.disabled = false;
                    btn.innerHTML = '<i class="fa-solid fa-bolt"></i> Inject Incident Alert';
                }, 4000);
            }
        }

        async function resetBlackboard() {
            await fetch('/api/reset', {method: 'POST'});
            fetchState();
        }

        setInterval(fetchState, 1000);
        fetchState();
    </script>
</body>
</html>
"""

class DashboardServer(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif parsed.path == "/api/state":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            leaderboard = bb.get_leaderboard()
            hypotheses = bb.get_all_hypotheses()
            timeline = bb.get_timeline()
            verdict = bb.get_final_verdict() or ""
            agent_steps = bb.get_agent_steps()

            contributors = {}
            for hid in hypotheses:
                contributors[hid] = bb.get_contributors(hid)

            stream_events = []
            try:
                raw_stream = bb.client.xrevrange("stream:rca:events", count=10)
                for msg_id, fields in raw_stream:
                    stream_events.append({
                        "id": msg_id,
                        "type": fields.get("type", "EVENT"),
                        "payload": fields
                    })
            except Exception:
                pass

            state = {
                "leaderboard": leaderboard,
                "hypotheses": hypotheses,
                "contributors": contributors,
                "timeline": timeline,
                "stream_events": stream_events,
                "agent_steps": agent_steps,
                "verdict": verdict
            }
            self.wfile.write(json.dumps(state).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/reset":
            bb.reset_blackboard()
            bb.init_stream()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')

        elif parsed.path == "/api/trigger":
            content_len = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_len)
            data = json.loads(body.decode("utf-8")) if body else {}
            scenario_id = data.get("scenario", "scenario_cache_ttl")

            bb.reset_blackboard()
            bb.init_stream()

            threading.Thread(target=run_swarm_in_thread, args=(scenario_id,), daemon=True).start()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "triggered"}')
        else:
            self.send_response(404)
            self.end_headers()

def run_swarm_in_thread(scenario_id: str):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    from agent_swarm import main as swarm_main
    loop.run_until_complete(swarm_main(scenario_id=scenario_id, trigger_incident=True))
    loop.close()

def run_dashboard_server(port: int = 8085):
    server = HTTPServer(("0.0.0.0", port), DashboardServer)
    print(f"🚀 Visual Educational Blackboard Dashboard running on http://lakshyagg.c.googlers.com:{port}")
    server.serve_forever()

if __name__ == "__main__":
    import sys
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8085
    run_dashboard_server(port)
