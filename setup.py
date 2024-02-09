from setuptools import setup, find_packages

setup(
    name="python_test",
    version="0.1.0",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "python_test = python_test.__main__:main"
        ]
    },
)
