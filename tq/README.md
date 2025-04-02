# tq

[![python-build-and-test](https://github.com/turquoisehealth/pricepoints/actions/workflows/python-build-and-test.yaml/badge.svg)](https://github.com/turquoisehealth/pricepoints/actions/workflows/python-build-and-test.yaml)
[![pre-commit](https://github.com/turquoisehealth/pricepoints/actions/workflows/pre-commit.yaml/badge.svg)](https://github.com/turquoisehealth/pricepoints/actions/workflows/pre-commit.yaml)

**tq** is a Python software package developed to support the
[Price Points](../README.md) research effort. It contains helper functions,
connector classes, and utilities to simplify working with Turquoise Health
data and endpoints.

For detailed documentation on included functions and data, [**visit the
full reference list**](https://turquoisehealth.github.io/pricepoints/reference.html).

## Installation

You can install the development version of `tq` using pip:

```python
pip install "git+https://git@github.com/turquoisehealth/pricepoints.git#subdirectory=tq"
```

Or via SSH:

```python
pip install "git+ssh://git@github.com/turquoisehealth/pricepoints.git#subdirectory=tq"
```

Once it's installed, you can use it just like any other package. Simply
call `import tq` at the beginning of your script.
