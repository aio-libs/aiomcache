STORED = b'STORED'
NOT_STORED = b'NOT_STORED'
TOUCHED = b'TOUCHED'
NOT_FOUND = b'NOT_FOUND'
DELETED = b'DELETED'
VERSION = b'VERSION'
EXISTS = b'EXISTS'
OK = b'OK'

# memcached's maximum key length, in bytes.
MAX_KEY_LENGTH = 250

# memcached reads an exptime of up to 30 days as seconds from now, and a larger
# one as a Unix timestamp.
MAX_RELATIVE_EXPTIME = 30 * 24 * 60 * 60
# memcached parses exptime as a signed 32-bit integer, so its Unix timestamps
# end at 2038-01-19 03:14:07 UTC.
MAX_EXPTIME = 2**31 - 1
