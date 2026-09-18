from metro_eval.coinc.gui_pyside import start


def main(args):
    start()


def parser(subparsers):
    parser = subparsers.add_parser(
        "coinc",
        description="Interactive analysis of coincidence data.",
    )

    parser.set_defaults(func=main)
