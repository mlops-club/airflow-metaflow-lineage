"""Common utility functions for the forecasting pipeline."""

from typing import Any
from jinja2 import DebugUndefined, Template


def substitute_map_into_string(string: str, values: dict[str, Any]) -> str:
    """Format a string using a dictionary with Jinja2 templating.

    :param string: The template string containing placeholders
    :param values: A dictionary of values to substitute into the template
    """
    template = Template(string, undefined=DebugUndefined)
    return template.render(values)
