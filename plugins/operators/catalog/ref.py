from enum import Enum
from typing import Any, Optional

from airflow.sdk import BaseOperator
from airflow.utils.context import Context
from hook.nessie import NessieHook

DEFAULT_BRANCH_NAME_TEMPLATE: str = "{{ params.branch_name | default('feature-' ~ (dag.dag_id | replace('_', '-')) ~ '-' ~ ts_nodash) }}"  # noqa: E501


class RefActionType(Enum):
    CREATE = "create"
    DELETE = "delete"
    MERGE = "merge"


class NessieRefOperator(BaseOperator):
    template_fields = ("branch_name", "ref", "onto_branch")

    def __init__(
        self,
        *,
        action: RefActionType = RefActionType.CREATE,
        branch_name: Optional[str] = None,
        ref: Optional[str] = None,
        onto_branch: Optional[str] = None,
        nessie_conn_id: str = "nessie_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.action = action
        self.branch_name = branch_name or DEFAULT_BRANCH_NAME_TEMPLATE
        self.ref = ref
        self.onto_branch = onto_branch
        self.nessie_conn_id = nessie_conn_id

    def execute(self, context: Context) -> Any:
        hook = NessieHook(nessie_conn_id=self.nessie_conn_id)

        rendered_branch_name = self.branch_name
        if isinstance(rendered_branch_name, str) and "{{" in rendered_branch_name:
            rendered_branch_name = context["ti"].render_template(
                rendered_branch_name, context
            )

        import re

        match = re.match(r"(feature-[\w-]+)-(\d{8})[-T]?(\d{6})", rendered_branch_name)
        if match:
            rendered_branch_name = f"{match.group(1)}-{match.group(2)}T{match.group(3)}"

        sanitized_branch_name = (
            rendered_branch_name.replace(":", "-")
            .replace("+", "-")
            .replace(" ", "-")
            .replace("T", "T")  # preserve T for Spark compatibility
        )

        if self.action == RefActionType.CREATE:
            self.log.info(
                f"Creating Nessie branch: {sanitized_branch_name} from base ref: {self.ref or 'default'}"  # noqa: E501
            )
            hook.create_branch(branch_name=sanitized_branch_name, ref=self.ref)
            self.log.info(f"Branch {sanitized_branch_name} created.")

            return sanitized_branch_name

        elif self.action == RefActionType.MERGE:
            target_branch = self.onto_branch or "default"
            self.log.info(
                f"Merging Nessie branch: {sanitized_branch_name} into {target_branch}"
            )
            hook.merge_branch(
                from_ref=sanitized_branch_name, onto_branch=self.onto_branch
            )
            self.log.info(
                f"Branch {sanitized_branch_name} merged into {target_branch}."
            )

            return self.onto_branch

        elif self.action == RefActionType.DELETE:
            self.log.info(f"Deleting Nessie branch: {sanitized_branch_name}")
            hook.delete_branch(branch_name=sanitized_branch_name)
            self.log.info(f"Branch {sanitized_branch_name} deleted.")
        return None
