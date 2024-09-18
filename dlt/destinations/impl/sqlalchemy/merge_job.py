from typing import Sequence, Tuple, Optional, List

import sqlalchemy as sa

from dlt.destinations.sql_jobs import SqlMergeFollowupJob, SqlJobParams
from dlt.common.destination.reference import PreparedTableSchema
from dlt.destinations.impl.sqlalchemy.db_api_client import SqlalchemyClient
from dlt.common.schema.utils import (
    get_columns_names_with_prop,
    get_dedup_sort_tuple,
    get_first_column_name_with_prop,
    is_nested_table,
)


class SqlalchemyMergeFollowupJob(SqlMergeFollowupJob):
    @classmethod
    def generate_sql(
        cls,
        table_chain: Sequence[PreparedTableSchema],
        sql_client: SqlalchemyClient,  # type: ignore[override]
        params: Optional[SqlJobParams] = None,
    ) -> List[str]:

        root_table = table_chain[0]

        root_table_obj = sql_client.get_existing_table(root_table["name"])
        staging_root_table_obj = root_table_obj.to_metadata(
            sql_client.metadata, schema=sql_client.staging_dataset_name
        )

        primary_key_names = get_columns_names_with_prop(root_table, "primary_key")
        merge_key_names = get_columns_names_with_prop(root_table, "merge_key")

        temp_metadata = sa.MetaData()

        # TODO: Handle append fallback, leaving for now to not nest the code so much

        key_clause = cls._generate_key_table_clauses(
            primary_key_names, merge_key_names, root_table_obj, staging_root_table_obj
        )

        sqla_statements = []

        # Generate the delete statements
        if len(table_chain) == 1 and not cls.requires_temp_table_for_delete():
            delete_statement = root_table_obj.delete().where(
                sa.exists(
                    sa.select([sa.literal(1)]).where(key_clause).select_from(staging_root_table_obj)
                )
            )
            sqla_statements.append(delete_statement)
        else:
            row_key_col_name = cls._get_row_key_col(table_chain, sql_client, root_table)
            row_key_col = root_table_obj.c[row_key_col_name]
            # Use a real table cause sqlalchemy doesn't have TEMPORARY TABLE abstractions
            delete_temp_table = sa.Table(
                # TODO: Generate uuid name
                "delete_" + root_table_obj.name,
                temp_metadata,
                row_key_col.copy(),
            )
            # Add the CREATE TABLE statement
            sqla_statements.append(sa.sql.ddl.CreateTable(delete_temp_table))
            # Insert data into the "temporary" table
            insert_statement = delete_temp_table.insert().from_select(
                [row_key_col],
                sa.select([row_key_col]).where(
                    sa.exists(
                        sa.select([sa.literal(1)])
                        .where(key_clause)
                        .select_from(staging_root_table_obj)
                    )
                ),
            )
            sqla_statements.append(insert_statement)

            for table in table_chain[1:]:
                chain_table_obj = sql_client.get_existing_table(table["name"])
                root_key_name = cls._get_root_key_col(table_chain, sql_client, table)
                root_key_col = chain_table_obj.c[root_key_name]

                delete_statement = chain_table_obj.delete().where(
                    root_key_col.in_(sa.select([row_key_col]).select_from(delete_temp_table))
                )

                sqla_statements.append(delete_statement)

            # Delete from root table
            delete_statement = root_table_obj.delete().where(
                row_key_col.in_(sa.select([row_key_col]).select_from(delete_temp_table))
            )
            sqla_statements.append(delete_statement)

        # TODO: Hard-delete information? What is it?
        # TODO: Dedupe sort information
        hard_delete_col_name, not_delete_cond = cls._get_hard_delete_col_and_cond(
            root_table,
            root_table_obj,
            invert=True,
        )

        dedup_sort = get_dedup_sort_tuple(root_table)

        if len(table_chain) > 1 and (primary_key_names or hard_delete_col_name is not None):
            condition_column_names = (
                None if hard_delete_col_name is None else [hard_delete_col_name]
            )
            staging_row_key_col = staging_root_table_obj.c[row_key_col_name]
            # Create the insert "temporary" table (but use a concrete table)

            insert_temp_table = sa.Table(
                "insert_" + root_table_obj.name,
                temp_metadata,
                staging_row_key_col.copy(),
            )

            create_insert_temp_table_statement = sa.sql.ddl.CreateTable(insert_temp_table)
            sqla_statements.append(create_insert_temp_table_statement)

            if primary_key_names:
                raise NotImplementedError("not yet")
                pass
            else:
                select_for_temp_insert = sa.select(staging_row_key_col).where(not_delete_cond)

            insert_into_temp_table = insert_temp_table.insert().from_select(
                [row_key_col_name], select_for_temp_insert
            )
            sqla_statements.append(insert_into_temp_table)

        # Insert from staging to dataset
        for table in table_chain:
            table_obj = sql_client.get_existing_table(table["name"])
            staging_table_obj = table_obj.to_metadata(
                sql_client.metadata, schema=sql_client.staging_dataset_name
            )

            insert_cond = not_delete_cond if hard_delete_col_name is not None else sa.true()

            if (primary_key_names and len(table_chain) > 1) or (
                not primary_key_names
                and is_nested_table(table)
                and hard_delete_col_name is not None
            ):
                uniq_column_name = root_key_name if is_nested_table(table) else row_key_col_name
                uniq_column = table_obj.c[uniq_column_name]
                insert_cond = uniq_column.in_(insert_temp_table.select().subquery())

            select_sql = staging_table_obj.select().where(insert_cond)
            if primary_key_names and len(table_chain) == 1:
                raise NotImplementedError("not yet")

            insert_statement = table_obj.insert().from_select(
                [col.name for col in table_obj.columns], select_sql
            )
            sqla_statements.append(insert_statement)

        return [
            x + ";" if not x.endswith(";") else x
            for x in (
                str(stmt.compile(sql_client.engine, compile_kwargs={"literal_binds": True}))
                for stmt in sqla_statements
            )
        ]

    @classmethod
    def _get_hard_delete_col_and_cond(  # type: ignore[override]
        cls,
        table: PreparedTableSchema,
        table_obj: sa.Table,
        invert: bool = False,
    ) -> Tuple[Optional[str], Optional[sa.sql.elements.BinaryExpression]]:
        col_name = get_first_column_name_with_prop(table, "hard_delete")
        if col_name is None:
            return None, None
        col = table_obj.c[col_name]
        if invert:
            cond = col.is_(None)
        else:
            cond = col.isnot(None)
        if table["columns"][col_name]["data_type"] == "bool":
            if invert:
                cond = sa.or_(cond, col.is_(False))
            else:
                cond = col.is_(True)
        return col_name, cond

    @classmethod
    def _generate_key_table_clauses(
        cls,
        primary_keys: Sequence[str],
        merge_keys: Sequence[str],
        root_table_obj: sa.Table,
        staging_root_table_obj: sa.Table,
    ) -> sa.sql.ClauseElement:
        # Returns an sqlalchemy or_ clause
        clauses = []
        if primary_keys or merge_keys:
            for key in primary_keys:
                clauses.append(
                    sa.and_(
                        *[
                            root_table_obj.c[key] == staging_root_table_obj.c[key]
                            for key in primary_keys
                        ]
                    )
                )
            for key in merge_keys:
                clauses.append(
                    sa.and_(
                        *[
                            root_table_obj.c[key] == staging_root_table_obj.c[key]
                            for key in merge_keys
                        ]
                    )
                )
            return sa.or_(*clauses)  # type: ignore[no-any-return]
        else:
            return sa.true()  # type: ignore[no-any-return]
