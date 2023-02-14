from hyprxa.exceptions import HyprxaError



class BrokerError(HyprxaError):
    """Base exception for all data integration errors."""


class SubscriptionError(BrokerError):
    """Raised when a broker failed to subscribe to subscriptions."""


class SubscriptionLimitError(SubscriptionError):
    """Raised by a broker when the maximum subscribers exist on the broker."""


class SubscriptionTimeout(SubscriptionError):
    """Raised by a broker when the timeout limit to subscribe is reached."""


class BrokerClosed(BrokerError):
    """Raised when attempting to subscribe to a broker instance is closed."""


class DroppedSubscriber(BrokerError):
    """Rraised when a subscriber has been stopped by the broker while the
    subscriber is being iterated.
    """