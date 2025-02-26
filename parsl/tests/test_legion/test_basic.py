import pytest

from parsl.app.app import python_app
from parsl.app.app import bash_app
from parsl.app.errors import AppTimeout


@pytest.mark.legion
def test_python_app():
    @python_app
    def dummy(a, b):
        return a+b

    arr_x = []
    for i in range(10):
        arr_x.append(dummy(a=i, b=i))
    for i in range(10):
        assert arr_x[i].result() == 2*i
        
@bash_app
def echo_to_file(inputs=(), outputs=(), walltime=0.01):
    return """echo "sleeping"; sleep 0.05"""


@pytest.mark.legion
def test_walltime():
    """Testing walltime exceeded exception """
    x = echo_to_file()
    with pytest.raises(AppTimeout):
        x.result()

@pytest.mark.legion
def test_walltime_longer():
    """Test that an app that runs in less than walltime will succeed."""
    y = echo_to_file(walltime=0.2)
    y.result()
