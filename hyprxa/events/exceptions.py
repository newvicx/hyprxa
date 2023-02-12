from hyprxa.integrations import ManagerClosed



class EventBusClosed(ManagerClosed):
    """Raised when attempting to subscribe to a closed event bus."""