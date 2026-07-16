# Pipelines

A pipeline is a named definition of source, transformations, checks, and sink.
Pipelines are created and edited in the Dataflow UI and are executed by the
background worker.

Each pipeline has a `run_as` role for data-access scoping, optional memory and
thread settings, optional checkpointing, and a template timezone.
