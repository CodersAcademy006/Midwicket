import argparse
import sys
from rich.console import Console
from rich.table import Table

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Midwicket: The Open Source Cricket Intelligence SDK",
        prog="midwicket"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Command: player
    player_parser = subparsers.add_parser("player", help="Get player career stats")
    player_parser.add_argument("name", type=str, help="Name of the player (e.g. 'V Kohli')")
    player_parser.add_argument("--type", type=str, choices=["batting", "bowling"], default="batting", help="Type of stats")

    # Command: batters
    batters_parser = subparsers.add_parser("batters", help="Show batting leaderboard")
    batters_parser.add_argument("--limit", type=int, default=20, help="Number of players to show")

    # Command: bowlers
    bowlers_parser = subparsers.add_parser("bowlers", help="Show bowling leaderboard")
    bowlers_parser.add_argument("--limit", type=int, default=20, help="Number of players to show")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)

    console = Console()
    
    with console.status("[bold green]Loading Midwicket engine...[/bold green]"):
        import midwicket as md
        
    try:
        if args.command == "player":
            if args.type == "batting":
                res = md.career_batting(args.name)
            else:
                res = md.career_bowling(args.name)
                
            # Trigger the rich repr automatically by printing it directly
            print(res)
            
        elif args.command == "batters":
            res = md.batting_leaderboard(limit=args.limit)
            print(res)
            
        elif args.command == "bowlers":
            res = md.bowling_leaderboard(limit=args.limit)
            print(res)
            
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
