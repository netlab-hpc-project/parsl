from parsl.app.app import join_app, python_app


@python_app
def add(*args):
    """Add all of the arguments together. If no arguments, then
    zero is returned (the neutral element of +)
    """
    accumulator = 0
    for v in args:
        accumulator += v
    return accumulator


@join_app
def fibonacci(n):
    if n == 0:
        return add()
    elif n == 1:
        return add(1)
    else:
        return add(fibonacci(n - 1), fibonacci(n - 2))


def test_fibonacci():
    print("Testing fibonacci(0):", fibonacci(0).result())
    assert fibonacci(0).result() == 0
    
    print("Testing fibonacci(4):", fibonacci(4).result())
    assert fibonacci(4).result() == 3
    
    print("Testing fibonacci(6):", fibonacci(6).result())
    assert fibonacci(6).result() == 8
    
    # print("Testing fibonacci(20):", fibonacci(20).result())
    # assert fibonacci(20).result() == 6765
    
    # print("Testing fibonacci(25):", fibonacci(25).result())
    # assert fibonacci(25).result() == 75025