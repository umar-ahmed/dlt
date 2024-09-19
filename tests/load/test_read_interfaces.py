import pytest
import dlt
import os

from dlt import Pipeline

from typing import List
from functools import reduce

from tests.load.utils import (
    destinations_configs,
    DestinationTestConfiguration,
    AZ_BUCKET,
    ABFS_BUCKET,
)
from pandas import DataFrame


def _run_dataset_checks(
    pipeline: Pipeline, destination_config: DestinationTestConfiguration
) -> None:
    destination_type = pipeline.destination_client().config.destination_type

    skip_df_chunk_size_check = False
    expected_columns = ["id", "_dlt_load_id", "_dlt_id"]
    if destination_type == "bigquery":
        chunk_size = 50
        total_records = 80
    elif destination_type == "mssql":
        chunk_size = 700
        total_records = 1000
    else:
        chunk_size = 2048
        total_records = 3000

    if destination_type == "snowflake":
        expected_columns = [e.upper() for e in expected_columns]

    # on filesystem one chunk is one file and not the default vector size
    if destination_type == "filesystem":
        skip_df_chunk_size_check = True

    # we always expect 2 chunks based on the above setup
    expected_chunk_counts = [chunk_size, total_records - chunk_size]

    @dlt.source()
    def source():
        @dlt.resource()
        def items():
            yield from [
                {"id": i, "children": [{"id": i + 100}, {"id": i + 1000}]}
                for i in range(total_records)
            ]

        return [items]

    # run source
    s = source()
    pipeline.run(s, loader_file_format=destination_config.file_format)

    # access via key
    relationship = pipeline.dataset()["items"]

    # full frame
    df = relationship.df()
    assert len(df.index) == total_records

    #
    # check dataframes
    #

    # chunk
    df = relationship.df(chunk_size=chunk_size)
    if not skip_df_chunk_size_check:
        assert len(df.index) == chunk_size
    assert set(df.columns.values) == set(expected_columns)

    # iterate all dataframes
    frames = list(relationship.iter_df(chunk_size=chunk_size))
    if not skip_df_chunk_size_check:
        assert [len(df.index) for df in frames] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [f[expected_columns[0]].to_list() for f in frames])
    assert set(ids) == set(range(total_records))

    # access via prop
    relationship = pipeline.dataset().items

    #
    # check arrow tables
    #

    # full table
    table = relationship.arrow()
    assert table.num_rows == total_records

    # chunk
    table = relationship.arrow(chunk_size=chunk_size)
    assert set(table.column_names) == set(expected_columns)
    assert table.num_rows == chunk_size

    # check frame amount and items counts
    tables = list(relationship.iter_arrow(chunk_size=chunk_size))
    assert [t.num_rows for t in tables] == expected_chunk_counts

    # check all items are present
    ids = reduce(lambda a, b: a + b, [t.column(expected_columns[0]).to_pylist() for t in tables])
    assert set(ids) == set(range(total_records))

    # check fetch accessors
    relationship = pipeline.dataset().items

    # check accessing one item
    one = relationship.fetchone()
    assert one[0] in range(total_records)

    # check fetchall
    fall = relationship.fetchall()
    assert len(fall) == total_records
    assert {item[0] for item in fall} == set(range(total_records))

    # check fetchmany
    many = relationship.fetchmany(chunk_size)
    assert len(many) == chunk_size

    # check iterfetchmany
    chunks = list(relationship.iter_fetchmany(chunk_size=chunk_size))
    assert [len(chunk) for chunk in chunks] == expected_chunk_counts
    ids = reduce(lambda a, b: a + b, [[item[0] for item in chunk] for chunk in chunks])
    assert set(ids) == set(range(total_records))

    # simple check that query also works
    relationship = pipeline.dataset().query("select * from items where id < 20")

    # we selected the first 20
    table = relationship.arrow()
    assert table.num_rows == 20


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(default_sql_configs=True),
    ids=lambda x: x.name,
)
def test_read_interfaces_sql(destination_config: DestinationTestConfiguration) -> None:
    pipeline = destination_config.setup_pipeline(
        "read_pipeline", dataset_name="read_test", dev_mode=True
    )
    _run_dataset_checks(pipeline, destination_config)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(
        local_filesystem_configs=True,
        all_buckets_filesystem_configs=True,
        bucket_exclude=[AZ_BUCKET, ABFS_BUCKET],
    ),  # TODO: make AZ work
    ids=lambda x: x.name,
)
def test_read_interfaces_filesystem(destination_config: DestinationTestConfiguration) -> None:
    # we force multiple files per table, they may only hold 50 items
    os.environ["DATA_WRITER__FILE_MAX_ITEMS"] = "700"

    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    _run_dataset_checks(pipeline, destination_config)


@pytest.mark.essential
@pytest.mark.parametrize(
    "destination_config",
    destinations_configs(local_filesystem_configs=True),
    ids=lambda x: x.name,
)
def test_evolving_filesystem(destination_config: DestinationTestConfiguration) -> None:
    """test that files with unequal schemas still work together"""

    if destination_config.file_format not in ["parquet", "jsonl"]:
        pytest.skip(
            f"Test only works for jsonl and parquet, given: {destination_config.file_format}"
        )

    @dlt.resource(table_name="items")
    def items():
        yield from [{"id": i} for i in range(20)]

    pipeline = destination_config.setup_pipeline(
        "read_pipeline",
        dataset_name="read_test",
        dev_mode=True,
    )

    pipeline.run([items()], loader_file_format=destination_config.file_format)

    df = pipeline.dataset().items.df()
    assert len(df.index) == 20

    @dlt.resource(table_name="items")
    def items2():
        yield from [{"id": i, "other_value": "Blah"} for i in range(20, 50)]

    pipeline.run([items2()], loader_file_format=destination_config.file_format)
    # check df and arrow access
    assert len(pipeline.dataset().items.df().index) == 50
    assert pipeline.dataset().items.arrow().num_rows == 50