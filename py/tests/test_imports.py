"""Every module compiles and imports. The command-line modules are otherwise only exercised by a
run, and a syntax error in one once reached a commit unnoticed."""

import importlib
import pkgutil

import northwind


def test_every_module_imports():
    names = [m.name for m in pkgutil.iter_modules(northwind.__path__)]
    assert "compare" in names
    for name in names:
        importlib.import_module(f"northwind.{name}")
