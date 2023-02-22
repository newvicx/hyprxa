from hyprxa.base import ManagerClosed, SubscriptionError
from hyprxa._exceptions import HyprxaError



class TimeseriesError(HyprxaError):
    """Base error for timeseries related errors."""
    

class SubscriptionLockError(SubscriptionError):
    """Raised by a manager after it failed to acquire locks for subscriptions
    due to an exception.
    """


class IntegrationSubscriptionError(SubscriptionError):
    """Raised by a manager when a integration failed to subscribe to subscriptions."""


class TimeseriesManagerClosed(ManagerClosed):
    """Raised when attempting to subscribe to a closed manager."""


class IntegrationClosed(TimeseriesError):
    """Raised when certain methods are called on a closed integration."""