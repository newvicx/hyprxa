class HyprxaError(Exception):
    """Base exception for all hyprxa errors."""


class NotConfiguredError(HyprxaError):
    """Raised when a feature is not enabled, usually due to a missing requirment
    in the config.
    """