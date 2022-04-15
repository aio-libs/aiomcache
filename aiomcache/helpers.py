import pickle  # noqa: S403
from enum import IntEnum
from typing import Any, Tuple


# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.h#L63
class PyLibMCFlags(IntEnum):
    PYLIBMC_FLAG_PICKLE = 1 << 0
    PYLIBMC_FLAG_INTEGER = 1 << 1
    PYLIBMC_FLAG_LONG = 1 << 2
    PYLIBMC_FLAG_BOOL = 1 << 4


# see PylibMC_deserialize_native in:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L640
async def pylibmc_get_flag_handler(value: bytes, flags: int) -> Any:
    if flags == PyLibMCFlags.PYLIBMC_FLAG_PICKLE:
        return pickle.loads(value)  # noqa: S301
    elif flags == PyLibMCFlags.PYLIBMC_FLAG_LONG:
        return int(value)
    elif flags == PyLibMCFlags.PYLIBMC_FLAG_BOOL:
        return bool(int(value))

    raise ValueError(f"unrecognized pylibmc flag: {flags}")


# see _PylibMC_serialize_native in:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L1241
async def pylibmc_set_flag_handler(value: Any) -> Tuple[bytes, int]:
    # bool is subclass of int so this needs to be first
    if isinstance(value, bool):
        return b'1' if value else b'0', PyLibMCFlags.PYLIBMC_FLAG_BOOL.value

    if isinstance(value, int):
        return str(value).encode('utf-8'), PyLibMCFlags.PYLIBMC_FLAG_LONG.value

    # default is pickle
    return pickle.dumps(value), PyLibMCFlags.PYLIBMC_FLAG_PICKLE.value
