from hyprxa.exceptions import HyprxaError



class IntegrationError(HyprxaError):
    """Base exception for all data integration errors."""


class SubscriptionError(IntegrationError):
    """Raised when a manager failed to subscribe to subscriptions."""


class SubscriptionLimitError(SubscriptionError):
    """Raised by a manager when the maximum subscribers exist on the manager."""


class SubscriptionTimeout(SubscriptionError):
    """Raised by a manager when the timeout limit to subscribe is reached."""


class SubscriptionLockError(SubscriptionError):
    """Raised by a manager after it failed to acquire locks for subscriptions
    as a result of a LockingError.
    """


class ClientSubscriptionError(SubscriptionError):
    """Raised by a manager when a client failed to subscribe to subscriptions."""


class ManagerClosed(IntegrationError):
    """Raised when attempting to subscribe to a manager instance is closed."""


class ClientClosed(IntegrationError):
    """Raised when a method is called on a closed client."""


class DroppedSubscriber(IntegrationError):
    """Rraised when a subscriber has been stopped by the manager while the
    subscriber is being iterated.
    """