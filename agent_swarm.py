"""
Autonomous Multi-Agent RCA Swarm Runner with Real Gemini 3.5 Flash Reasoning.
Launches the 4 specialized agents and SynthesizerArbiter daemon concurrently.
Supports 3 realistic incident scenarios and provides rich live terminal output.
"""
import asyncio
import sys
import argparse
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.markdown import Markdown
from blackboard import Blackboard
from llm_reasoner import LLMReasoner
from telemetry_mock import TelemetryMock
from agents import (
    TraceExplorerAgent,
    DatabaseSleuthAgent,
    DeployScoutAgent,
    InfraK8sAgent,
    SynthesizerArbiter
)

console = Console()

async def control_listener(blackboard: Blackboard, agents: list, synthesizer: SynthesizerArbiter):
    """Listens on Redis Pub/Sub rca:control:broadcast for shutdown signals."""
    pubsub = blackboard.get_pubsub_listener()
    loop = asyncio.get_running_loop()

    def get_message():
        return pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)

    while True:
        msg = await loop.run_in_executor(None, get_message)
        if msg and msg.get("type") == "message":
            data = msg.get("data")
            if data == "TERMINATE":
                for agent in agents:
                    agent.stop()
                synthesizer.running = False
                break
        await asyncio.sleep(0.1)

def generate_dashboard(blackboard: Blackboard) -> Table:
    """Renders the live blackboard state table."""
    table = Table(title="🧠 Valkey RCA Blackboard - Real-time Stigmergic State", border_style="cyan")
    table.add_column("Leaderboard (Rank & Score)", style="bold yellow", width=35)
    table.add_column("Status & Contributors", style="bold green", width=35)
    table.add_column("Recent Timeline Anomalies", style="white", width=45)

    leaderboard = blackboard.get_leaderboard()
    hypotheses = blackboard.get_all_hypotheses()
    timeline = blackboard.get_timeline()[-3:]

    col1_lines = []
    col2_lines = []
    if leaderboard:
        for rank, (hid, score) in enumerate(leaderboard, start=1):
            col1_lines.append(f"#{rank} [bold cyan]{hid}[/bold cyan]: {score:.1f} pts")
            hdata = hypotheses.get(hid, {})
            status = hdata.get("status", "UNKNOWN")
            contributors = blackboard.get_contributors(hid)
            col2_lines.append(f"[{status}] Contributors ({len(contributors)}): {', '.join(contributors)}")
    else:
        col1_lines.append("[dim]Waiting for agent triggers...[/dim]")
        col2_lines.append("[dim]No active hypotheses yet...[/dim]")

    col3_lines = []
    if timeline:
        for t in timeline:
            col3_lines.append(f"• [cyan]{t.get('service')}:[/cyan] {t.get('anomaly')[:38]}...")
    else:
        col3_lines.append("[dim]Awaiting telemetry anomalies...[/dim]")

    table.add_row("\n".join(col1_lines), "\n".join(col2_lines), "\n".join(col3_lines))
    return table

async def live_dashboard_task(blackboard: Blackboard, stop_event: asyncio.Event):
    with Live(generate_dashboard(blackboard), refresh_per_second=4, console=console) as live:
        while not stop_event.is_set():
            live.update(generate_dashboard(blackboard))
            await asyncio.sleep(0.25)
        live.update(generate_dashboard(blackboard))

async def main(scenario_id: str = "scenario_cache_ttl", trigger_incident: bool = False):
    console.print(Panel.fit(
        "[bold cyan]🐝 Autonomous Multi-Agent RCA Swarm with Gemini 3.5 Flash Reasoning[/bold cyan]\n"
        "[italic green]Emergent Stigmergic Root Cause Analysis via Valkey Blackboard[/italic green]",
        border_style="bright_blue"
    ))

    bb = Blackboard()
    if not bb.ping():
        console.print("[bold red]❌ Cannot connect to Valkey blackboard instance.[/bold red]")
        sys.exit(1)

    # Initialize LLM reasoner
    llm = LLMReasoner()
    console.print(f"[bold green]✔ Initialized Google GenAI SDK (Model: {llm.model_name})[/bold green]")

    # Initialize specialized agents
    trace_agent = TraceExplorerAgent(bb, llm)
    db_agent = DatabaseSleuthAgent(bb, llm)
    deploy_agent = DeployScoutAgent(bb, llm)
    infra_agent = InfraK8sAgent(bb, llm)
    agents = [trace_agent, db_agent, deploy_agent, infra_agent]

    synthesizer = SynthesizerArbiter(bb, llm, convergence_threshold=85.0, min_contributors=2, timeout_seconds=45)

    stop_event = asyncio.Event()

    tasks = [
        asyncio.create_task(trace_agent.run()),
        asyncio.create_task(db_agent.run()),
        asyncio.create_task(deploy_agent.run()),
        asyncio.create_task(infra_agent.run()),
        asyncio.create_task(control_listener(bb, agents, synthesizer)),
        asyncio.create_task(live_dashboard_task(bb, stop_event))
    ]

    console.print("[bold green]✔ All 4 specialized agents + Synthesizer daemon active and listening on Valkey Stream.[/bold green]")

    if trigger_incident:
        await asyncio.sleep(0.6)
        # Select scenario definition
        scenarios = {s["id"]: s for s in TelemetryMock.get_scenarios()}
        sc = scenarios.get(scenario_id, scenarios["scenario_cache_ttl"])

        console.print(f"\n[bold red]⚡ INJECTING INCIDENT ALERT into 'stream:rca:events':[/bold red] {sc['alert_type']} on '{sc['service']}' ({sc['name']})...")
        bb.emit_event(sc["alert_type"], {
            "service": sc["service"],
            "scenario": scenario_id,
            "error_message": sc["description"]
        })

    final_verdict = await synthesizer.monitor_and_arbitrate()

    stop_event.set()
    await asyncio.sleep(0.5)

    for t in tasks:
        t.cancel()

    console.print("\n" + "="*80 + "\n")
    if final_verdict:
        console.print(Markdown(final_verdict))
    else:
        cached = bb.get_final_verdict()
        if cached:
            console.print(Markdown(cached))
        else:
            console.print("[yellow]Session completed.[/yellow]")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Autonomous Multi-Agent RCA Swarm")
    parser.add_argument("--scenario", default="scenario_cache_ttl", choices=["scenario_cache_ttl", "scenario_k8s_oom", "scenario_db_deadlock"], help="Incident scenario to run")
    parser.add_argument("--trigger-incident", action="store_true", help="Inject incident alert on start")
    args = parser.parse_args()

    asyncio.run(main(scenario_id=args.scenario, trigger_incident=args.trigger_incident))
