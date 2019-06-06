import math


class ByteFactors:
    Byte_to_KB = 1024
    KB_to_MB = math.pow(Byte_to_KB, 2)
    KB_to_GB = math.pow(Byte_to_KB, 3)
    KB_to_TB = math.pow(Byte_to_KB, 4)

    MB_to_GB = 1024
    MB_to_TB = KB_to_MB

    GB_to_TB = 1024
