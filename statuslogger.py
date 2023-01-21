import logging

import markdown  # pypi: markdown

from markdown_link_attr_modifier import (
    LinkAttrModifierExtension,
)  # pypi: markdown-link-attr-modifier


class StatusFormatter(logging.Formatter):
    def format(self, record):
        """Output markdown status messages as HTML5."""
        return markdown.markdown(
            super().format(record),
            output_format="html5",
            extensions=[LinkAttrModifierExtension(new_tab="on")],
        )
