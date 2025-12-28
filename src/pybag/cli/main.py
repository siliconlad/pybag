import argparse

from pybag.cli import (
    mcap_convert,
    mcap_filter,
    mcap_info,
    mcap_merge,
    mcap_recover,
    mcap_sort
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pybag",
        description=(
            "Command line interface for pybag. Provides utilities to work with "
            "MCAP and bag files."
        ),
    )
    parser.set_defaults(func=lambda args: parser.print_help())

    # Pybag CLI Subcommands
    subparsers = parser.add_subparsers(dest="command")

    # TODO: Have some of entrypoint registration?
    mcap_convert.add_parser(subparsers)
    mcap_filter.add_parser(subparsers)
    mcap_merge.add_parser(subparsers)
    mcap_info.add_parser(subparsers)
    mcap_sort.add_parser(subparsers)
    mcap_recover.add_parser(subparsers)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
