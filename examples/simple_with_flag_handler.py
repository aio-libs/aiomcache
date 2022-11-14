import asyncio
import datetime
import pickle  # noqa: S403
from enum import IntEnum
from typing import Any, Tuple

import aiomcache


class SimpleFlags(IntEnum):
    DEMO_FLAG_PICKLE = 1


async def simple_get_flag_handler(value: bytes, flags: int) -> Any:
    print("get flag handler invoked")

    if flags == SimpleFlags.DEMO_FLAG_PICKLE:
        return pickle.loads(value)  # noqa: S301

    raise ValueError(f"unrecognized flag: {flags}")


async def simple_set_flag_handler(value: Any) -> Tuple[bytes, int]:
    print("set flag handler invoked")

    return pickle.dumps(value), SimpleFlags.DEMO_FLAG_PICKLE.value


async def hello_aiomcache_with_flag_handlers() -> None:
    mc = aiomcache.FlagClient("127.0.0.1", 11211,
                              get_flag_handler=simple_get_flag_handler,
                              set_flag_handler=simple_set_flag_handler)

    await mc.set(b"some_first_key", b"Some value")
    value = await mc.get(b"some_first_key")

    print(f"retrieved value {repr(value)} without flag handler")

    date_value = datetime.date(2015, 12, 28)

    # flag handlers only triggered for non-byte values
    await mc.set(b"some_key_with_flag_handlers", date_value)
    value = await mc.get(b"some_key_with_flag_handlers")

    print(f'retrieved value with flag handler: {repr(value)}')


asyncio.run(hello_aiomcache_with_flag_handlers())
