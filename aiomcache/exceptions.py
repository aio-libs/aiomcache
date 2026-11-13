__all__ = ['ClientException', 'ValidationException']


class ClientException(Exception):
    """Raised when the server does something we don't expect."""

    def __init__(self, msg: str, item: object | None = None):
        if item is not None:
            msg = f'{msg}: {item!r}'
        super().__init__(msg)


class ValidationException(ClientException):
    """Raised when an invalid parameter is passed to a ``Client`` function."""
