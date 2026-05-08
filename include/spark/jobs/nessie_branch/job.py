import argparse

from include.spark.common.nessie import create_branch, delete_branch, merge_branch
from include.spark.common.session_factory import SparkSessionFactory


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--action", required=True, choices=["create", "merge", "delete"]
    )
    parser.add_argument("--branch_name", required=True)
    parser.add_argument("--base_ref", required=False)
    parser.add_argument("--target_ref", required=False)
    return parser.parse_args()


def run_action(
    action: str, *, branch_name: str, base_ref: str | None, target_ref: str | None
):
    spark = SparkSessionFactory.create_session("NessieBranchOperator")

    if action == "create":
        create_branch(spark, branch_name=branch_name, from_branch_name=base_ref)
    elif action == "merge":
        if not target_ref:
            raise ValueError("target_ref is required for MERGE")
        merge_branch(spark, from_branch_name=branch_name, to_branch_name=target_ref)
    elif action == "delete":
        delete_branch(spark, branch_name=branch_name)


if __name__ == "__main__":
    args = parse_args()
    run_action(
        args.action,
        branch_name=args.branch_name,
        base_ref=args.base_ref,
        target_ref=args.target_ref,
    )
