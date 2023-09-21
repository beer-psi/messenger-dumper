import asyncio
import argparse
import importlib
import inspect
import os


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        "-d",
        "--database",
        type=str,
        required=False,
        default=os.path.join(os.path.dirname(__file__), "database", "database.sqlite3"),
        help="Database location",
    )

    subparsers = parser.add_subparsers(
        title='subcommands',
        description='valid subcommands'
    )

    for file in os.listdir("commands"):
        if file.startswith("__") or not file.endswith(".py"):
            continue
        try:
            mod = importlib.import_module(f"commands.{os.path.splitext(file)[0]}")
            if not hasattr(mod, "add_command") or not hasattr(mod, "execute"):
                continue
            if not inspect.iscoroutinefunction(mod.execute):
                continue
            subparser = mod.add_command(subparsers)
            subparser.set_defaults(func=mod.execute)
        except ImportError:
            pass

    args = parser.parse_args()
    asyncio.run(args.func(args))
