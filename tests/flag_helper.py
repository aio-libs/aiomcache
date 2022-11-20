import pickle  # noqa: S403
from enum import IntEnum
from typing import Any, Tuple


# See also:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.h#L63
class DemoFlags(IntEnum):
    DEMO_FLAG_PICKLE = 1


# demo/ref flag handler, for more elaborate potential handlers, see:
# https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L640
class FlagHelperDemo:

    get_invocation_count = 0
    set_invocation_count = 0

    async def demo_get_flag_handler(self, value: bytes, flags: int) -> Any:
        self.get_invocation_count += 1

        if flags == DemoFlags.DEMO_FLAG_PICKLE:
            return pickle.loads(value)  # noqa: S301

        raise ValueError(f"unrecognized flag: {flags}")

    # demo/ref flag handler, for more elaborate potential handlers, see:
    # https://github.com/lericson/pylibmc/blob/master/src/_pylibmcmodule.c#L1241
    async def demo_set_flag_handler(self, value: Any) -> Tuple[bytes, int]:
        self.set_invocation_count += 1

        # in this example exclusively use Pickle, more elaborate handler
        # could use additional/alternate flags
        return pickle.dumps(value), DemoFlags.DEMO_FLAG_PICKLE.value
