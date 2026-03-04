from setuptools import setup, find_packages

setup(
    name="sigil",
    version="0.1.0",
    packages=find_packages(),
    include_package_data=True,
    package_data={"sigil": ["grammar.tx", "templates/*.j2"]},
    install_requires=[
        "textX>=3.1.0",
        "Jinja2>=3.1.0",
    ],
    entry_points={
        "console_scripts": [
            "sigil=sigil.cli:main",
        ],
    },
    python_requires=">=3.10",
)
