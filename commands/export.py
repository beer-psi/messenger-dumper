def add_command(subparsers):
    export_parser = subparsers.add_parser(
        "export",
        help="export chat logs to viewer"
    )
    export_parser.add_argument(
        "-i",
        "--id",
        type=int,
        nargs="+",
        required=True,
        help="IDs of threads to export (the long string of number in the chat URL)",
    )
    return export_parser


async def execute(args):
    pass
