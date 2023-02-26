def get_routing_key(topic: str, routing_key: str | None, event: bool = False) -> str:
    """Build the routing key from the topic if required."""
    if routing_key and routing_key.startswith(topic):
        return ".".join([split for split in routing_key.split(".") if split])
    if routing_key:
        routing_key = ".".join([split for split in routing_key.split(".") if split])
        return f"{topic}.{routing_key}"
    else:
        if event:
            return topic
        return f"{topic}.#"