1. import 原则:

例如, 你有三个模块 ``module1.py``, ``module2.py``, ``module3.py``.

.. code-block:: python

    # content of module1.py
    def module1_func(): pass

.. code-block:: python

    # content of module2.py
    from .module1 import module1_func

    def module2_func():
        return module1_func()

.. code-block:: python

    # content of module3.py
    from .module1 import module1_func

    def module3_func():
        return module1_func()

在为 module2 写测试的时候, 如果你需要用到 ``module1_func``. 限制只能从当前被测试的模块 import, 也就是 ``from module2 import module1_func`` 因为这是你正在测试的模块. 或是从原始定义的模块 import, 也就是 ``from module1 import module1_func``. **不要从其他模块 import**, 比如不要从 module3 import. 原因是你正在给 module2 测试, 就专注于测试本身, 从其他地方 import 这个行为是不能被保证的, 很可能别人改一改就改没了. 关于 import 本身的测试用专门的 ``test_import.py`` 进行覆盖.


2. 在 pytest 写 unittest