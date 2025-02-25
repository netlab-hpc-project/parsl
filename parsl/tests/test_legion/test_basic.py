import pytest

import parsl
from parsl.config import Config
from parsl.app.app import python_app

def test_app():
    @python_app
    def dummy(a, b):
        return a+b

    x = dummy(a=7, b=1)
    assert x.result() == 8