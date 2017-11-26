class Tags:
    alloc = 0
    read = 1
    modify = 2
    free = 3
    sort = 4
    quit = 5

    @classmethod
    def name(cls, i):
        for k, v in cls.__dict__.items():
            if not k.startswith('__') and v == i:
                return k

        raise ValueError('Value {} not found in {}.'.format(i, cls))
