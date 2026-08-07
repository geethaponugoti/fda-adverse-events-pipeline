"""
Tool: reads the dbt manifest (dbt_project/target/manifest.json) to find
which staging/mart models are downstream of raw.fda_adverse_events, and
which of those actually reference a given column — so a schema_drift
alert on one column can be traced to the specific models it will break.
"""

import json
import os

DBT_MANIFEST_PATH = os.environ.get(
    "DBT_MANIFEST_PATH", "/opt/airflow/dbt_project/target/manifest.json"
)


def _load_manifest() -> dict:
    if not os.path.exists(DBT_MANIFEST_PATH):
        raise FileNotFoundError(
            f"dbt manifest not found at {DBT_MANIFEST_PATH} — run `dbt run` "
            f"or `dbt compile` at least once to generate it."
        )
    with open(DBT_MANIFEST_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_source_unique_id(manifest: dict, source_name: str, table_name: str):
    for unique_id, node in manifest.get("sources", {}).items():
        if node.get("source_name") == source_name and node.get("name") == table_name:
            return unique_id
    return None


def _all_downstream_ids(manifest: dict, root_unique_id: str) -> set:
    """child_map only gives direct children, so e.g. mart_drug_reactions
    (two hops from the source, via stg_fda_adverse_events) wouldn't show
    up from a single lookup. Walk the graph breadth-first to collect
    every transitively downstream node."""
    child_map = manifest.get("child_map", {})
    seen = set()
    queue = list(child_map.get(root_unique_id, []))

    while queue:
        node_id = queue.pop()
        if node_id in seen:
            continue
        seen.add(node_id)
        queue.extend(child_map.get(node_id, []))

    return seen


def get_impacted_models(
    source_name: str = "raw",
    table_name: str = "fda_adverse_events",
    column_name: str = None,
) -> dict:
    """Returns every dbt model transitively downstream of the given
    source table, and (if column_name is given) which of those models
    reference that column in their SQL — the ones actually at risk from
    the change."""
    manifest = _load_manifest()

    source_unique_id = _find_source_unique_id(manifest, source_name, table_name)
    if source_unique_id is None:
        return {
            "source_found": False,
            "impacted_models": [],
            "models_referencing_column": [],
            "note": f"source {source_name}.{table_name} not found in dbt manifest",
        }

    nodes = manifest.get("nodes", {})
    downstream_ids = _all_downstream_ids(manifest, source_unique_id)

    impacted_models = []
    models_referencing_column = []

    for node_id in downstream_ids:
        node = nodes.get(node_id)
        if not node or node.get("resource_type") != "model":
            continue

        model_name = node.get("name")
        impacted_models.append(model_name)

        if column_name:
            code = node.get("raw_code") or node.get("compiled_code") or ""
            if column_name.lower() in code.lower():
                models_referencing_column.append(model_name)

    return {
        "source_found": True,
        "impacted_models": sorted(impacted_models),
        "models_referencing_column": sorted(models_referencing_column),
    }
