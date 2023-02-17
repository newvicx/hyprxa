import inspect

from hyprxa.events.queries import find_one_event


print(inspect.signature(find_one_event).parameters)