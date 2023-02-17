from hyprxa.base.exceptions import BrokerClosed



class EventManagerClosed(BrokerClosed):
    """Raised when attempting to subscribe to a closed manager."""