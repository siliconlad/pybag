import argparse

from pybag.cli import mcap_filter


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

    from .merge import add_parser as add_merge_parser

    # `filter` command
    mcap_filter.add_parser(subparsers)

    # `merge` command
    add_merge_parser(subparsers)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
