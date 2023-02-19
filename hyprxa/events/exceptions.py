from hyprxa.base.exceptions import ManagerClosed



class EventManagerClosed(ManagerClosed):
    """Raised when attempting to subscribe to a closed event manager."""