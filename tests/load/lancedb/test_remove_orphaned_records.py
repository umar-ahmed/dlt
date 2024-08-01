from typing import Iterator, List, Generator

import pandas as pd
import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal

import dlt
from dlt.common.typing import DictStrAny
from dlt.common.utils import uniq_id
from dlt.destinations.impl.lancedb.lancedb_adapter import DOCUMENT_ID_HINT
from tests.load.utils import (
    drop_active_pipeline_data,
)
from tests.pipeline.utils import (
    assert_load_info,
)


# Mark all tests as essential, don't remove.
pytestmark = pytest.mark.essential


@pytest.fixture(autouse=True)
def drop_lancedb_data() -> Iterator[None]:
    yield
    drop_active_pipeline_data()


def test_lancedb_remove_orphaned_records() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(  # type: ignore[call-overload]
        table_name="parent",
        write_disposition="merge",
        columns={"id": {DOCUMENT_ID_HINT: True}},
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    run_1 = [
        {
            "id": 1,
            "child": [
                {"bar": 1, "grandchild": [{"baz": 1}, {"baz": 2}]},
                {"bar": 2, "grandchild": [{"baz": 3}]},
            ],
        },
        {"id": 2, "child": [{"bar": 3, "grandchild": [{"baz": 4}]}]},
        {
            "id": 3,
            "child": [
                {"bar": 10, "grandchild": [{"baz": 5}]},
                {"bar": 11, "grandchild": [{"baz": 6}, {"baz": 7}]},
            ],
        },
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {
            "id": 1,
            "child": [{"bar": 1, "grandchild": [{"baz": 1}]}],
        },  # Removed one child and one grandchild
        {
            "id": 2,
            "child": [{"bar": 4, "grandchild": [{"baz": 8}]}],
        },  # Changed child and grandchild
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_child_data = pd.DataFrame(
            data=[
                {"bar": 1},
                {"bar": 4},
                {"bar": 10},
                {"bar": 11},
            ]
        )

        expected_grandchild_data = pd.DataFrame(
            data=[
                {"baz": 1},
                {"baz": 8},
                {"baz": 5},
                {"baz": 6},
                {"baz": 7},
            ]
        )

        child_table_name = client.make_qualified_table_name("parent__child")  # type: ignore[attr-defined]
        grandchild_table_name = client.make_qualified_table_name(  # type: ignore[attr-defined]
            "parent__child__grandchild"
        )

        child_tbl = client.db_client.open_table(child_table_name)  # type: ignore[attr-defined]
        grandchild_tbl = client.db_client.open_table(grandchild_table_name)  # type: ignore[attr-defined]

        actual_child_df = (
            child_tbl.to_pandas()
            .sort_values(by="bar")
            .reset_index(drop=True)
            .reset_index(drop=True)
        )
        actual_grandchild_df = (
            grandchild_tbl.to_pandas()
            .sort_values(by="baz")
            .reset_index(drop=True)
            .reset_index(drop=True)
        )

        expected_child_data = expected_child_data.sort_values(by="bar").reset_index(drop=True)
        expected_grandchild_data = expected_grandchild_data.sort_values(by="baz").reset_index(
            drop=True
        )

        assert_frame_equal(actual_child_df[["bar"]], expected_child_data)
        assert_frame_equal(actual_grandchild_df[["baz"]], expected_grandchild_data)


def test_lancedb_remove_orphaned_records_root_table() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="test_lancedb_remove_orphaned_records_root_table",
        destination="lancedb",
        dataset_name=f"test_lancedb_remove_orphaned_records_root_table_{uniq_id()}",
        dev_mode=True,
    )

    @dlt.resource(  # type: ignore[call-overload]
        table_name="root",
        write_disposition="merge",
        merge_key=["chunk_hash"],
        columns={"doc_id": {DOCUMENT_ID_HINT: True}},
    )
    def identity_resource(
        data: List[DictStrAny],
    ) -> Generator[List[DictStrAny], None, None]:
        yield data

    run_1 = [
        {"doc_id": 1, "chunk_hash": "1a"},
        {"doc_id": 2, "chunk_hash": "2a"},
        {"doc_id": 2, "chunk_hash": "2b"},
        {"doc_id": 2, "chunk_hash": "2c"},
        {"doc_id": 3, "chunk_hash": "3a"},
        {"doc_id": 3, "chunk_hash": "3b"},
    ]
    info = pipeline.run(identity_resource(run_1))
    assert_load_info(info)

    run_2 = [
        {"doc_id": 2, "chunk_hash": "2d"},
        {"doc_id": 2, "chunk_hash": "2e"},
        {"doc_id": 3, "chunk_hash": "3b"},
    ]
    info = pipeline.run(identity_resource(run_2))
    assert_load_info(info)

    with pipeline.destination_client() as client:
        expected_root_table_df = (
            pd.DataFrame(
                data=[
                    {"doc_id": 1, "chunk_hash": "1a"},
                    {"doc_id": 2, "chunk_hash": "2d"},
                    {"doc_id": 2, "chunk_hash": "2e"},
                    {"doc_id": 3, "chunk_hash": "3b"},
                ]
            )
            .sort_values(by=["doc_id", "chunk_hash"])
            .reset_index(drop=True)
        )

        root_table_name = client.make_qualified_table_name("root")  # type: ignore[attr-defined]
        tbl = client.db_client.open_table(root_table_name)  # type: ignore[attr-defined]

        actual_root_df: DataFrame = (
            tbl.to_pandas().sort_values(by=["doc_id", "chunk_hash"]).reset_index(drop=True)
        )[["doc_id", "chunk_hash"]]

        assert_frame_equal(actual_root_df, expected_root_table_df)
