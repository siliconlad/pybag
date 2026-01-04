import argparse

from pybag.cli import convert, filter, info, inspect, merge, recover, sort


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
    convert.add_parser(subparsers)
    filter.add_parser(subparsers)
    info.add_parser(subparsers)
    inspect.add_parser(subparsers)
    merge.add_parser(subparsers)
    recover.add_parser(subparsers)
    sort.add_parser(subparsers)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
