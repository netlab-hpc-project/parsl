import pytest
from parsl.app.app import bash_app


@bash_app
def mpirun_app(inputs=(), outputs=()):
    return """mpirun -np 8  /home/netlab/xlc_interview/project/flexflow/target/develop/examples/cpp/normal_legion/normallegion -level 3 -ll:cpu 1 -ll:csize 1024 -ll:max_dyn_csize 1024"""

@pytest.mark.legion
def test_mpirun_app():
    """Test that an app that runs in less than walltime will succeed."""
    y = mpirun_app()
    print(y.result())