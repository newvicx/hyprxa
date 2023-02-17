import random



class BaseBackoff:
    """Backoff interface"""
    def __init__(self) -> None:
        self.failures = 0

    def reset(self):
        """Reset internal state before an operation."""
        self.failures = 0

    def compute(self):
        """Compute backoff in seconds upon failure."""
        raise NotImplementedError()


class EqualJitterBackoff(BaseBackoff):
    """Equal jitter backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial
        super().__init__()

    def reset(self):
        super().reset()

    def compute(self):
        self.failures += 1
        temp = min(self.cap, self.initial * 2**self.failures) / 2
        return temp + random.uniform(0, temp)


class DecorrelatedJitterBackoff(BaseBackoff):
    """Decorrelated jitter backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial
        self._previous_backoff = 0

    def reset(self):
        self._previous_backoff = 0

    def compute(self):
        max_backoff = max(self.initial, self._previous_backoff * 3)
        temp = random.uniform(self.initial, max_backoff)
        self._previous_backoff = min(self.cap, temp)
        return self._previous_backoff


class ConstantBackoff(BaseBackoff):
    """Constant backoff upon failure."""

    def __init__(self, backoff: float):
        self.backoff = backoff
        super().__init__()

    def compute(self):
        return self.backoff


class ExponentialBackoff(BaseBackoff):
    """Exponential backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial

    def reset(self):
        super().reset()
    
    def compute(self):
        self.failures += 1
        return min(self.cap, self.initial * 2**self.failures)


class FullJitterBackoff(BaseBackoff):
    """Full jitter backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial

    def reset(self):
        super().reset()

    def compute(self):
        self.failures += 1
        return random.uniform(0, min(self.cap, self.initial * 2**self.failures))