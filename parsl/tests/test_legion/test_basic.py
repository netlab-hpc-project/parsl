import pytest

import parsl
from parsl.config import Config
from parsl.app.app import python_app
from parsl.executors.legion.executor import LegionExecutor


    

@python_app
def dummy(a, b):
    return a+b

@pytest.mark.legion
def test_app():
    x = dummy(7, b=1)
    assert x.result() == 8
    parsl.dfk().cleanup()