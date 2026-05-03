from typing import TYPE_CHECKING, List, Optional, Union

from great_expectations.checkpoint import Checkpoint
from great_expectations.data_context import AbstractDataContext

from include.spark.common.gx.common import classproperty


class GXCheckpoint:
    """
    Generate GXCheckpoint with Builder Pattern
     - Example Usage:
        checkpoint = (
            GXCheckpoint.builder.name("my_checkpoint")
            .suites("my_suite")
            .store_validation_result()
            .update_data_docs()
            .build_and_update(context)
        )
     - Required Parameters: name, suites

    """

    if TYPE_CHECKING:
        builder: "GXCheckpoint.Builder"

    class Builder:
        def __init__(self):
            self._checkpoint_name: Optional[str] = None
            self._validations: List[dict] = []
            self._actions: List[dict] = []

        def name(self, checkpoint_name: str) -> "GXCheckpoint.Builder":
            self._checkpoint_name = checkpoint_name
            return self

        def suites(self, suite_names: Union[str, List[str]]) -> "GXCheckpoint.Builder":
            self._validations = [
                {
                    "expectation_suite_name": suite_name,
                }
                for suite_name in (
                    suite_names if isinstance(suite_names, list) else [suite_names]
                )
            ]
            return self

        def _add_action(self, name: str, class_name: str, **kwargs):
            self._actions.append(
                {
                    "name": name,
                    "action": {"class_name": class_name, **kwargs},
                }
            )
            return self

        def store_validation_result(self) -> "GXCheckpoint.Builder":
            self._add_action("store_validation_result", "StoreValidationResultAction")
            return self

        def update_data_docs(self) -> "GXCheckpoint.Builder":
            self._add_action("update_data_docs", "UpdateDataDocsAction")
            return self

        def build_and_update(self, context: AbstractDataContext) -> Checkpoint:
            if not self._checkpoint_name:
                raise ValueError("Checkpoint name is not set. Call .name() first.")
            if not self._validations:
                raise ValueError("Suite names are not set. Call .suites() first.")

            if not self._actions:
                self.store_validation_result()
                self.update_data_docs()

            checkpoint = context.add_or_update_checkpoint(
                name=self._checkpoint_name,
                validations=self._validations,
                action_list=self._actions,
            )
            return checkpoint

    @classproperty
    def builder(cls) -> Builder:
        return cls.Builder()
