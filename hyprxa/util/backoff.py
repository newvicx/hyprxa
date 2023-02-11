from abc import ABC, abstractmethod
import random



class AbstractBackoff(ABC):
    """Backoff interface"""

    def reset(self):
        """Reset internal state before an operation."""
        pass

    @abstractmethod
    def compute(self, failures: int):
        """Compute backoff in seconds upon failure."""
        pass


class EqualJitterBackoff(AbstractBackoff):
    """Equal jitter backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial

    def compute(self, failures: int):
        temp = min(self.cap, self.initial * 2**failures) / 2
        return temp + random.uniform(0, temp)


class DecorrelatedJitterBackoff(AbstractBackoff):
    """Decorrelated jitter backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial
        self._previous_backoff = 0

    def reset(self):
        self._previous_backoff = 0

    def compute(self, failures: int):
        max_backoff = max(self.initial, self._previous_backoff * 3)
        temp = random.uniform(self.initial, max_backoff)
        self._previous_backoff = min(self.cap, temp)
        return self._previous_backoff


class ConstantBackoff(AbstractBackoff):
    """Constant backoff upon failure."""

    def __init__(self, backoff: float):
        self.backoff = backoff

    def compute(self, _: int):
        return self.backoff


class ExponentialBackoff(AbstractBackoff):
    """Exponential backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial

    def compute(self, failures: int):
        return min(self.cap, self.initial * 2**failures)


class FullJitterBackoff(AbstractBackoff):
    """Full jitter backoff upon failure."""

    def __init__(self, cap: float, initial: float):
        self.cap=cap
        self.initial = initial

    def compute(self, failures: int):
        return random.uniform(0, min(self.cap, self.initial * 2**failures))