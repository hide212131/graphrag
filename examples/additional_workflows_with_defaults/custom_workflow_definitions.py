from datashaper import DEFAULT_INPUT_NAME, AsyncType

from graphrag.index.config.workflow import PipelineWorkflowConfig, PipelineWorkflowStep

def build_create_extracted_entities(
    config: PipelineWorkflowConfig,
) -> list[PipelineWorkflowStep]:
    """
    Create the base table for extracted entities.

    ## Dependencies
    * `workflow:create_base_text_units`
    """
    entity_extraction_config = config.get("entity_extract", {})
    graphml_snapshot_enabled = config.get("graphml_snapshot", False) or False
    raw_entity_snapshot_enabled = config.get("raw_entity_snapshot", False) or False

    return [
        {
            "verb": "entity_extract",
            "args": {
                **entity_extraction_config,
                "column": entity_extraction_config.get("text_column", "chunk"),
                "id_column": entity_extraction_config.get("id_column", "chunk_id"),
                "async_mode": entity_extraction_config.get(
                    "async_mode", AsyncType.AsyncIO
                ),
                "to": "entities",
                "graph_to": "entity_graph",
            },
            "input": {"source": "workflow:create_base_text_units"},
        },
        {
            "verb": "snapshot",
            "enabled": raw_entity_snapshot_enabled,
            "args": {
                "name": "raw_extracted_entities",
                "formats": ["json"],
            },
        },
        {
            "verb": "merge_graphs",
            "args": {
                "column": "entity_graph",
                "to": "entity_graph",
                **config.get(
                    "graph_merge_operations",
                    {
                        "nodes": {
                            "source_id": {
                                "operation": "concat",
                                "delimiter": ", ",
                                "distinct": True,
                            },
                            "description": ({
                                "operation": "concat",
                                "separator": "\n",
                                "distinct": False,
                            }),
                        },
                        "edges": {
                            "source_id": {
                                "operation": "concat",
                                "delimiter": ", ",
                                "distinct": True,
                            },
                            "description": ({
                                "operation": "concat",
                                "separator": "\n",
                                "distinct": False,
                            }),
                            "weight": "sum",
                        },
                    },
                ),
            },
        },
        {
            "verb": "snapshot_rows",
            "enabled": graphml_snapshot_enabled,
            "args": {
                "base_name": "merged_graph",
                "column": "entity_graph",
                "formats": [{"format": "text", "extension": "graphml"}],
            },
        },
    ]


def build_create_base_documents_steps(
    config: PipelineWorkflowConfig,
) -> list[PipelineWorkflowStep]:
    """
    Create the documents table.

    ## Dependencies
    * `workflow:create_final_text_units`
    """
    document_attribute_columns = config.get("document_attribute_columns", [])
    return [
        {
            "verb": "unroll",
            "args": {"column": "document_ids"},
            "input": {"source": "workflow:create_final_text_units"},
        },
        {
            "verb": "select",
            "args": {
                # We only need the chunk id and the document id
                "columns": ["id", "document_ids", "text"]
            },
        },
        {
            "id": "rename_chunk_doc_id",
            "verb": "rename",
            "args": {
                "columns": {
                    "document_ids": "chunk_doc_id",
                    "id": "chunk_id",
                    "text": "chunk_text",
                }
            },
        },
        {
            "verb": "join",
            "args": {
                # Join the doc id from the chunk onto the original document
                "on": ["chunk_doc_id", "id"]
            },
            "input": {"source": "rename_chunk_doc_id", "others": [DEFAULT_INPUT_NAME]},
        },
        {
            "id": "docs_with_text_units",
            "verb": "aggregate_override",
            "args": {
                "groupby": ["id"],
                "aggregations": [
                    {
                        "column": "chunk_id",
                        "operation": "array_agg",
                        "to": "text_units",
                    }
                ],
            },
        },
        {
            "verb": "join",
            "args": {
                "on": ["id", "id"],
                "strategy": "right outer",
            },
            "input": {
                "source": "docs_with_text_units",
                "others": [DEFAULT_INPUT_NAME],
            },
        },
        {
            "verb": "rename",
            "args": {"columns": {"text": "raw_content"}},
        },
        *[
            {
                "verb": "convert",
                "args": {
                    "column": column,
                    "to": column,
                    "type": "string",
                },
            }
            for column in document_attribute_columns
        ],
        {
            "verb": "merge_override",
            "enabled": len(document_attribute_columns) > 0,
            "args": {
                "columns": document_attribute_columns,
                "strategy": "json",
                "to": "attributes",
            },
        },
        {"verb": "convert", "args": {"column": "id", "to": "id", "type": "string"}},
    ]


def build_create_base_text_units_steps(
    config: PipelineWorkflowConfig,
) -> list[PipelineWorkflowStep]:
    """
    Create the base table for text units.

    ## Dependencies
    None

             {
            # .py
            "verb": "filter",
            "args": {
                "column": "title",
                "criteria": [
                    {
                        "type": "value",
                        "operator": "ends with",
                        "value": ".py",
                    }
                ],
            },
        }, 
    """
    chunk_column_name = config.get("chunk_column", "chunk")
    chunk_by_columns = config.get("chunk_by", []) or []
    n_tokens_column_name = config.get("n_tokens_column", "n_tokens")
    return [
        {
            "verb": "orderby",
            "args": {
                "orders": [
                    # sort for reproducibility
                    {"column": "id", "direction": "asc"},
                ]
            },
            "input": {"source": DEFAULT_INPUT_NAME},
        },
        {
            "verb": "zip",
            "args": {
                # Pack the document ids with the text
                # So when we unpack the chunks, we can restore the document id
                "columns": ["id", "text"],
                "to": "text_with_ids",
            },
        },
        {
            "verb": "aggregate_override",
            "args": {
                "groupby": [*chunk_by_columns] if len(chunk_by_columns) > 0 else None,
                "aggregations": [
                    {
                        "column": "text_with_ids",
                        "operation": "array_agg",
                        "to": "texts",
                    }
                ],
            },
        },
        {
            "verb": "chunk",
            "args": {"column": "texts", "to": "chunks", **config.get("text_chunk", {})},
        },
        {
            "verb": "select",
            "args": {
                "columns": [*chunk_by_columns, "chunks"],
            },
        },
        {
            "verb": "unroll",
            "args": {
                "column": "chunks",
            },
        },
        {
            "verb": "rename",
            "args": {
                "columns": {
                    "chunks": chunk_column_name,
                }
            },
        },
        {
            "verb": "genid",
            "args": {
                # Generate a unique id for each chunk
                "to": "chunk_id",
                "method": "md5_hash",
                "hash": [chunk_column_name],
            },
        },
        {
            "verb": "unzip",
            "args": {
                "column": chunk_column_name,
                "to": ["document_ids", chunk_column_name, n_tokens_column_name],
            },
        },
        {"verb": "copy", "args": {"column": "chunk_id", "to": "id"}},
        {
            # ELIMINATE EMPTY CHUNKS
            "verb": "filter",
            "args": {
                "column": chunk_column_name,
                "criteria": [
                    {
                        "type": "value",
                        "operator": "is not empty",
                    }
                ],
            },
        },
        ## REPLACE "初めまして" to "やあ！" in the chunks column
        {
            "verb": "my_text_replace",
            "args": {
                "column": chunk_column_name,
                "to": chunk_column_name,
                "replacements": [{
                    "pattern": "初めまして",
                    "replacement": "オス！",
                }],
            }
        },        
    ]


