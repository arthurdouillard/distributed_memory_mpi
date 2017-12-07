#!/usr/bin/env python3

import random

import distributed_memory as dm

mem = None

def test(fun):
    def wrap():
        print(fun.__name__.ljust(30), end='')
        try:
            fun()
        except:
            print('FAILED')
        else:
            print('OK')
    return wrap


@test
def test_add_int():
    original = 42
    var = mem.add(original)
    value = mem.read(var)
    assert value == original
    mem.free(var)


@test
def test_add_list_small():
    original = [1, 2, 3, 4]
    var = mem.add(original)
    value = mem.read(var)
    assert value == original
    mem.free(var)


@test
def test_add_list_big():
    original = list(range(15))
    var = mem.add(original)
    value = mem.read(var)
    assert value == original
    mem.free(var)


@test
def test_modify_int():
    original = 42
    var = mem.add(original)
    mem.modify(var, 1337)
    value = mem.read(var)
    assert value == 1337
    mem.free(var)


@test
def test_modify_list_small():
    var = mem.add([1, 2, 3])
    b = mem.modify(var, 42, index=2)
    assert b == True
    value = mem.read(var)
    assert value == [1, 2, 42]
    mem.free(var)


@test
def test_modify_list_big():
    var = mem.add(list(range(15)))
    b = mem.modify(var, 42, index=12)
    assert b == True
    value = mem.read(var)
    l = list(range(15))
    l[12] = 42
    assert value == l
    mem.free(var)


@test
def test_free_int():
    var = mem.add(42)
    mem.free(var)
    assert not var
    assert len(var.var_names) == 0


@test
def test_free_small_list():
    var = mem.add(list(range(5)))
    mem.free(var)
    assert not var
    assert len(var.var_names) == 0


@test
def test_free_big_list():
    var = mem.add(list(range(15)))
    mem.free(var)
    assert not var
    assert len(var.var_names) == 0


@test
def test_free_many():
    for _ in range(100):
        i = random.randint(1, 17)
        var = mem.add(list(range(i)))
        assert var
        mem.free(var)
        assert not var
        assert all(v == 0 for v in mem.slaves_tracking.values())


@test
def test_map_small_list():
    var = mem.add(list(range(5)))
    mem.map(var, lambda x: x ** 2)
    value = mem.read(var)
    assert value == list(map(lambda x: x ** 2, list(range(5))))
    mem.free(var)


@test
def test_map_big_list():
    var = mem.add(list(range(15)))
    mem.map(var, lambda x: x ** 2)
    value = mem.read(var)
    assert value == list(map(lambda x: x ** 2, list(range(15))))
    mem.free(var)


@test
def test_reduce_small_list():
    var = mem.add(list(range(5)))
    value = mem.reduce(var, lambda x, y: x+y, 0)
    assert value == sum(range(5))
    mem.free(var)


@test
def test_reduce_big_list():
    var = mem.add(list(range(15)))
    value = mem.reduce(var, lambda x, y: x+y, 0)
    assert value == sum(range(15))
    mem.free(var)


@test
def test_filter_small_list():
    var = mem.add(list(range(5)))
    mem.filter(var, lambda x: x % 2 == 0)
    value = mem.read(var)
    assert value == list(filter(lambda x: x % 2 == 0, list(range(5))))
    mem.free(var)


@test
def test_filter_big_list():
    var = mem.add(list(range(15)))
    mem.filter(var, lambda x: x % 2 == 0)
    value = mem.read(var)
    assert value == list(filter(lambda x: x % 2 == 0, list(range(15))))
    mem.free(var)


@test
def test_filter_free_small():
    var = mem.add(list(range(5)))
    mem.filter(var, lambda x: x < 0)
    assert not var
    assert len(var.var_names) == 0


@test
def test_filter_free_big():
    var = mem.add(list(range(15)))
    mem.filter(var, lambda x: x < 0)
    assert not var
    assert len(var.var_names) == 0


def main():
    test_add_int()
    test_add_list_small()
    test_add_list_big()
    test_modify_int()
    test_modify_list_small()
    test_modify_list_big()
    test_free_int()
    test_free_small_list()
    test_free_big_list()
    test_free_many()
    test_map_small_list()
    test_map_big_list()
    test_reduce_small_list()
    test_reduce_big_list()
    test_filter_small_list()
    test_filter_big_list()
    test_filter_free_small()
    test_filter_free_big()


if __name__ == '__main__':
    mem = dm.init_memory(max_per_slave=10)
    main()
    mem.quit()