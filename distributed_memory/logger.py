""""Module for custom logging of class' methods."""

def log(msg):
    def wrapper(f):
        def wrap(self, *args, **kwargs):
            self.log(pretty_log(msg, self, args, kwargs))
            return f(self, *args, *kwargs)
        return wrap
    return wrapper


def pretty_log(msg, self, *args):
    args, kwargs = args[0], args[1]

    pretty_msg = ' {}'.format(msg)

    arg, kwarg = '', ''
    if len(args) != 0:
        arg = ', '.join(list(map(str, args)))
        pretty_msg = '{}: {}'.format(pretty_msg, arg)

    if len(kwargs) != 0:
        kwarg_list = []
        for k, v in kwargs.items():
            kwarg_list.append('{}={}'.format(k, v))
        kwarg = ', '.join(kwarg_list)
        pretty_msg = '{}: {}'.format(pretty_msg, kwarg)

    return pretty_msg
