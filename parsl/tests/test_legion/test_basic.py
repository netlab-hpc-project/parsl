import pytest

import parsl
from parsl.config import Config
from parsl.app.app import python_app

def test_app():
    @python_app
    def dummy(a, b):
        return a+b

    arr_x = []
    for i in range(10):
        arr_x.append(dummy(a=i, b=i))
    for i in range(10):
        assert arr_x[i].result() == 2*i