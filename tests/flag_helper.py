import pickle  # noqa: S403
from enum import IntEnum
from typing import Any, Tuple


# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.h#L63
class DemoFlags(IntEnum):
    DEMO_FLAG_PICKLE = 1 << 0

# demo/ref flag handler, for more elaborate potential handlers, see:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L640
async def demo_get_flag_handler(value: bytes, flags: int) -> Any:
    if flags == DemoFlags.DEMO_FLAG_PICKLE:
        return pickle.loads(value)  # noqa: S301

    raise ValueError(f"unrecognized flag: {flags}")


# demo/ref flag handler, for more elaborate potential handlers, see:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L1241
async def demo_set_flag_handler(value: Any) -> Tuple[bytes, int]:
    # in this example exclusively use Pickle, more elaborate handler
    # could use additional/alternate flags
    return pickle.dumps(value), DemoFlags.DEMO_FLAG_PICKLE.value
