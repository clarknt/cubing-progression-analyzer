from enum import Enum, unique

@unique
class EventId(Enum):
    """
    Supported WCA events
    """

    # 3x3x3 Cube
    E_333 = '333'

    # 2x2x2 Cube
    E_222 = '222'

    # 4x4x4 Cube
    E_444 = '444'

    # 5x5x5 Cube
    E_555 = '555'

    # 6x6x6 Cube
    E_666 = '666'

    # 7x7x7 Cube
    E_777 = '777'

    # 3x3x3 blindfolded
    E_333_BF = '333bf'

    # 3x3x3 Fewest Moves
    # Not supported
    #E_333_FM = '333fm'

    # 3x3x3 One-Handed
    E_333_OH = '333oh'

    # 3x3x3 With Feet
    E_333_FT = '333ft'

    # Clock
    E_CLOCK = 'clock'

    # Megaminx
    E_MEGAMINX = 'minx'

    # Pyraminx
    E_PYRAMINX = 'pyram'

    # Skewb
    E_SKEWB = 'skewb'

    # Square-1
    E_SQUARE_1 = 'sq1'

    # 444 Blindfolded
    E_444_BF = '444bf'

    # 5x5x5 blindfolded
    E_555_BF = '555bf'

    # 3x3x3 Multi-Blind
    # Not supported
    #E_333_MB = '333mbf'    

    # 3x3x3 Multi-Blind Old Style
    # Not supported
    #E_333_MBO = '333mbo'

    # Master Magic
    E_MASTER_MAGIC = 'mmagic'