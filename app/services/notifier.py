from typing import Optional


class Notifier:
    """Placeholder notifier to be swapped with email, SMS or webhook providers."""

    def __init__(self, sink: Optional[str] = None):
        self.sink = sink or "stdout"

    def notify(self, destination: str, message: str):
        # In production this could send an email or push notification.
        print(f"[notify -> {destination}] {message}")
