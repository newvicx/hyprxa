from hyprxa.base import BrokerClosed, SubscriptionError
from hyprxa.exceptions import HyprxaError



class SubscriptionLockError(SubscriptionError):
    """Raised by a manager after it failed to acquire locks for subscriptions
    as a result of a LockingError.
    """


class ClientSubscriptionError(SubscriptionError):
    """Raised by a manager when a client failed to subscribe to subscriptions."""


class ManagerClosed(BrokerClosed):
    """Raised when attempting to subscribe to a manager instance is closed."""


class TimeseriesError(HyprxaError):
    """Base error for timeseries related errors."""


class ClientClosed(TimeseriesError):
    """Raised when a method is called on a closed client."""