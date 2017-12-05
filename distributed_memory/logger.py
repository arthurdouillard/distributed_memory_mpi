import logging

def log(msg):
    def wrapper(f):
        def wrap(self, *args, **kwargs):
            self.log('Clock: {}\t'.format(self.clock.timer) +\
                    msg + ': {} {}'.format(args, kwargs))
            f(self, *args, *kwargs)
        return wrap
    return wrapper