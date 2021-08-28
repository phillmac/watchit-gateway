from src.core.commands import transcode, nft, ingest

HANDLERS = {   # TODO implement command handlers
    'transcode': lambda **kwargs: None,
    'ingest': lambda **kwargs: None,
    'mint': lambda **kwargs: None
}

class QueueItem(dict):
    def __init__(self, **kwargs) -> None:
        self.update(kwargs)

    @property
    def params(self):
        return self.copy()

    @property
    def command(self):
        return self.get('command', None)

    @property
    def args(self):
        return self.get('args', None)

    @property
    def priority(self):
        return self.get('priority', 100)

    def __lt__(self, other):
        return self.priority < other.priority

    def process(self):
        if self.command is None:
            return

        handler = HANDLERS.get(self.command, None)
        if handler is None:
            raise NotImplementedError(f"Command {self.command} not available")
        handler(**self.params)
