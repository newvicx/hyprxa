from typing import Dict


def set_routing_key(v: Dict[str, str | None]) -> Dict[str, str]:
    """Set the routing key for the event. This is a root validator."""
    topic = v.get("topic")
    routing_key = v.get("routing_key")
    v["routing_key"] = get_routing_key(topic, routing_key)
    return v


def get_routing_key(topic: str, routing_key: str | None) -> str:
    """Build the routing key from the topic if required."""
    if routing_key and routing_key.startswith(topic):
        return routing_key
    if routing_key:
        routing_key = ".".join([split for split in routing_key.split(".") if split])
        return f"{topic}.{routing_key}"
    else:
        return f"{topic}.#"