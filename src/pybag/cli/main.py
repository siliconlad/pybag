import argparse
from pathlib import Path

from pybag.cli.structure import structure_command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pybag",
        description=(
            "Command line interface for pybag. Provides utilities to work with "
            "MCAP and bag files."
        ),
    )
    subparsers = parser.add_subparsers(dest="command")

    structure_parser = subparsers.add_parser(
        "structure",
        help="Display a visual representation of the records contained in an MCAP file.",
    )
    structure_parser.add_argument(
        "mcap",
        type=Path,
        help="Path to the MCAP file to inspect.",
    )
    structure_parser.set_defaults(func=structure_command)


    parser.set_defaults(func=lambda args: parser.print_help())
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
