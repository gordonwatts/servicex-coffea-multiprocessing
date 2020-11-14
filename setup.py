from setuptools import setup

extras_require = {}
extras_require["lint"] = ["flake8"]
extras_require["test"] = ["pytest"]
extras_require["develop"] = sorted(
    set(extras_require["lint"] + extras_require["test"] + ["pre-commit", " jupyterlab"])
)
extras_require["complete"] = sorted(set(sum(extras_require.values(), [])))

setup(
    extras_require=extras_require,
)
