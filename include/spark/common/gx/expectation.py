from typing import TYPE_CHECKING, List, Optional, Union

from great_expectations.core import ExpectationConfiguration

from include.spark.common.gx.common import classproperty


class GXExpectation:
    if TYPE_CHECKING:
        builder: "GXExpectation.Builder"

    class Builder:
        def __init__(self):
            self._expectations: List[ExpectationConfiguration] = []

        def _add_expectation(
            self, expectation_type: str, kwargs: dict, meta: Optional[dict] = None
        ):
            config = {
                "expectation_type": expectation_type,
                "kwargs": kwargs,
            }
            if meta:
                config["meta"] = meta

            self._expectations.append(ExpectationConfiguration(**config))

        def table_row_count_between(
            self,
            min_value: int = 0,
            max_value: Optional[int] = None,
            meta: Optional[dict] = None,
            **kwargs,
        ) -> "GXExpectation.Builder":
            ex_kwargs = {"min_value": min_value, "max_value": max_value, **kwargs}
            self._add_expectation(
                "expect_table_row_count_to_be_between", ex_kwargs, meta
            )
            return self

        def column_not_null(
            self, columns: Union[str, List[str]], meta: Optional[dict] = None, **kwargs
        ) -> "GXExpectation.Builder":
            if isinstance(columns, str):
                columns = [columns]

            for col in columns:
                ex_kwargs = {"column": col, **kwargs}
                self._add_expectation(
                    "expect_column_values_to_not_be_null", ex_kwargs, meta
                )
            return self

        def column_greater_than_zero(
            self, columns: Union[str, List[str]], meta: Optional[dict] = None, **kwargs
        ) -> "GXExpectation.Builder":
            if isinstance(columns, str):
                columns = [columns]

            for col in columns:
                ex_kwargs = {
                    "column": col,
                    "min_value": 0,
                    "strict_min": True,
                    **kwargs,
                }
                self._add_expectation(
                    "expect_column_values_to_be_between", ex_kwargs, meta
                )
            return self

        def column_value_between(
            self,
            columns: Union[str, List[str]],
            min_value: Optional[int] = None,
            max_value: Optional[int] = None,
            meta: Optional[dict] = None,
            **kwargs,
        ) -> "GXExpectation.Builder":
            if isinstance(columns, str):
                columns = [columns]

            for col in columns:
                ex_kwargs = {"column": col, **kwargs}
                if min_value is not None:
                    ex_kwargs["min_value"] = min_value
                if max_value is not None:
                    ex_kwargs["max_value"] = max_value

                self._add_expectation(
                    "expect_column_values_to_be_between", ex_kwargs, meta
                )
            return self

        def column_unique(
            self, columns: Union[str, List[str]], meta: Optional[dict] = None, **kwargs
        ) -> "GXExpectation.Builder":
            if isinstance(columns, str):
                columns = [columns]

            for col in columns:
                ex_kwargs = {"column": col, **kwargs}
                self._add_expectation(
                    "expect_column_values_to_be_unique", ex_kwargs, meta
                )
            return self

        def build(self) -> List[ExpectationConfiguration]:
            if not self._expectations:
                raise ValueError("No expectations have been added to the builder.")

            return self._expectations

    @classproperty
    def builder(cls) -> Builder:
        return cls.Builder()
