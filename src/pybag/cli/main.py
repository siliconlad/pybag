import argparse

from . import mcap_filter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pybag",
        description=(
            "Command line interface for pybag. Provides utilities to work with "
            "MCAP and bag files."
        ),
    )
    subparsers = parser.add_subparsers(dest="command")

    mcap_parser = subparsers.add_parser("mcap", help="Utilities for MCAP files")
    mcap_subparsers = mcap_parser.add_subparsers(dest="mcap_command")

    mcap_filter.add_parser(mcap_subparsers)

    parser.set_defaults(func=lambda args: parser.print_help())
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
