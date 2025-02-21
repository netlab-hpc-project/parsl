Parsl - Legion Execution增强 x86版本
==================================

- 如何修改和使用
    - 第一步: ./tag_and_release.sh reinstall
    - 第二步: pytest parsl/tests/test_python_apps/test_fibonacci_recursive.py --config parsl/tests/configs/legion.py -s (此时会发现出现大量的错误, 把这些错误解决了, 并将Parsl和Legion完成联调就完成工作了)
- 核心代码位置
    - ./parsl/executors/legion/