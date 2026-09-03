"""
Harness Setup script for Autonomous Multi-Agent RCA Swarm.
Resets the Redis/Valkey Blackboard keyspace and initializes stream consumer groups.
"""
import sys
import time
from blackboard import Blackboard
from rich.console import Console

console = Console()

def setup_harness():
    console.print("[bold cyan]🔧 Initializing RCA Multi-Agent Blackboard Harness...[/bold cyan]")
    try:
        bb = Blackboard()
        if not bb.ping():
            console.print("[bold red]❌ Cannot connect to Redis/Valkey at localhost:6379[/bold red]")
            sys.exit(1)
        
        console.print("[green]✔ Connected to Redis/Valkey instance.[/green]")
        
        # Reset state
        bb.reset_blackboard()
        console.print("[green]✔ Blackboard keyspace reset (streams, hypotheses, confidence leaderboard, locks cleared).[/green]")
        
        # Initialize stream and consumer group
        bb.init_stream(group_name="agents-group", stream_name="stream:rca:events")
        console.print("[green]✔ Stream 'stream:rca:events' and Consumer Group 'agents-group' initialized.[/green]")
        
        console.print("[bold green]✨ Harness setup complete! Swarm is ready for incident injection.[/bold green]\n")
    except Exception as e:
        console.print(f"[bold red]❌ Failed during harness setup: {e}[/bold red]")
        sys.exit(1)

if __name__ == "__main__":
    setup_harness()
