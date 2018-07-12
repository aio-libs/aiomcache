from enum import IntEnum
import pickle


class PyLibMCFlags(IntEnum):
    PYLIBMC_FLAG_PICKLE = 1 << 0
    PYLIBMC_FLAG_INTEGER = 1 << 1
    PYLIBMC_FLAG_LONG = 1 << 2
    PYLIBMC_FLAG_BOOL = 1 << 4


# see PylibMC_deserialize_native in:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L640
async def pylibmc_flag_handler(value, flags):
    if flags == PyLibMCFlags.PYLIBMC_FLAG_PICKLE:
        return pickle.loads(value)
    elif flags == PyLibMCFlags.PYLIBMC_FLAG_LONG:
        return int(value)
    elif flags == PyLibMCFlags.PYLIBMC_FLAG_BOOL:
        return bool(int(value))
    else:
        assert False
