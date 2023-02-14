from hyprxa.base import BrokerClosed



class EventBusClosed(BrokerClosed):
    """Raised when attempting to subscribe to a closed event bus."""