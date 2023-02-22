from hyprxa._exceptions import HyprxaError



class ManagerError(HyprxaError):
    """Base exception for all manager errors."""


class SubscriptionError(ManagerError):
    """Raised when a manager failed to subscribe to subscriptions."""


class SubscriptionLimitError(SubscriptionError):
    """Raised by a manager when the maximum subscribers exist on the manager."""


class SubscriptionTimeout(SubscriptionError):
    """Raised by a manager when the timeout limit to subscribe is reached."""


class ManagerClosed(ManagerError):
    """Raised when attempting to subscribe to a closed manager."""


class DroppedSubscriber(ManagerError):
    """Rraised when a subscriber has been stopped by the manager while the
    subscriber is being iterated.
    """