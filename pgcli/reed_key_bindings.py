import logging
from prompt_toolkit.key_binding import KeyBindings

_logger = logging.getLogger(__name__)


def add_custom_key_bindings(kb, pgcli):

    @kb.add("c-e")
    def _(event):
        """Edit the current input in external editor."""
        _logger.debug("Detected <C-e> key.")
        buff = event.app.current_buffer
        buff.insert_text("\\e")
        buff.validate_and_handle()

    @kb.add("c-b")
    def _(event):
        """Select active schema"""
        _logger.debug("Detected <C-b> key.")
        buff = event.app.current_buffer
        buff.insert_text("\\ss")
        buff.validate_and_handle()
