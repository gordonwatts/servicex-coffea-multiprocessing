from setuptools import setup

extras_require = {}
extras_require["lint"] = ["flake8"]
extras_require["test"] = ["pytest"]
extras_require["develop"] = sorted(
    set(extras_require["lint"] + extras_require["test"] + ["pre-commit", " jupyterlab"])
)
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))

setup(
    name="sx_multi",
    version="0.0.1",
    packages=['sx_multi'],
    install_requires=['aiostream', 'tenacity', 'servicex==2.2b5', 'servicex_clients', 'coffea', 'dill'],
    scripts=[],
    extras_require=extras_require
)
