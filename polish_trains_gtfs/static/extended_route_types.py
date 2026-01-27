from typing import cast
import impuls


class ApplyExtendedRouteTypes(impuls.Task):
    def execute(self, r: impuls.TaskRuntime) -> None:
        with r.db.transaction():
            version = cast(
                int, r.db.raw_execute("PRAGMA schema_version").one_must("")[0]
            )
            r.db.raw_execute("PRAGMA writable_schema=ON")
            r.db.raw_execute(
                r"UPDATE sqlite_schema SET sql=re_sub(' CHECK \(type IN \([^\0]+?\)\)', '', sql) "
                "WHERE type='table' AND name='routes'"
            )
            r.db.raw_execute(f"PRAGMA schema_version={version + 1}")
            r.db.raw_execute("PRAGMA writable_schema=OFF")
            r.db.raw_execute("PRAGMA integrity_check")

            r.db.raw_execute(
                """
                UPDATE routes
                SET type = json_extract(extra_fields_json, "$.extended_type")
                """
            )
