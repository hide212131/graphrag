# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License
import asyncio
import os

import pandas as pd
from graphrag.index.workflows.v1.create_base_documents import (
    workflow_name as create_base_documents,
)
from graphrag.index.workflows.v1.create_base_text_units import (
    workflow_name as create_base_text_units,
)
from graphrag.index.workflows.v1.create_base_extracted_entities import (
    workflow_name as create_base_extracted_entities,
)
from graphrag.index.workflows.default_workflows import (
    default_workflows
)
from examples.additional_workflows_with_defaults.custom_workflow_definitions import (
    build_create_base_documents_steps,
    build_create_base_text_units_steps,
    build_create_extracted_entities,
)
default_workflows[create_base_documents] = build_create_base_documents_steps
default_workflows[create_base_text_units] = build_create_base_text_units_steps
default_workflows[create_base_extracted_entities] = build_create_extracted_entities

from graphrag.index.cli import index_cli
from graphrag.index.emit.types import TableEmitterType
from graphrag.index.progress.types import ReporterType
  
if __name__ == "__main__":
    index_cli(
        root_dir="./ragtest",
        verbose=False,
        resume="",
        update_index_id=None,
        memprofile=False,
        nocache=False,
        reporter=ReporterType("rich"),
        config_filepath=None,
        emit=[TableEmitterType('parquet')],
        dryrun=False,
        init=False,
        skip_validations=False,
        output_dir=None
    )
