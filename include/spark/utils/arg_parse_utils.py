def parse_required_args(required_args: list[str]):
    import argparse

    parser = argparse.ArgumentParser()
    for arg_name in required_args:
        parser.add_argument(f"--{arg_name}", required=True)
    args, _ = parser.parse_known_args()
    return args
