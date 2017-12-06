""""Module implementing an enum-like object used for tags in MPI's messages."""

class Tags:
    alloc = 0
    read = 1
    modify = 2
    free = 3
    sort = 4
    quit = 5
    map = 6
    reduce = 7
    filter = 8

    @classmethod
    def name(cls, i):
        """Get enum string name by its value."""
        for k, v in cls.__dict__.items():
            if not k.startswith('__') and v == i:
                return k

        raise ValueError('Value {} not found in {}.'.format(i, cls))


    @classmethod
    def get_id(cls, name):
        """Get enum value by its string name."""
        return cls.__dict__[name]
