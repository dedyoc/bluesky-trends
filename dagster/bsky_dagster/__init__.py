"""bsky_dagster — the v2 batch/archive orchestration package.

Lives under the repo's ``dagster/`` directory but is imported as ``bsky_dagster`` (NOT
``dagster``) so the folder never shadows the installed ``dagster`` pip package. The
``dagster/`` directory deliberately has no ``__init__.py`` — it is a source root on the
path (see pyproject ``mypy_path`` / pytest ``pythonpath`` and ``make dagster``), not a
package.
"""
