"""Midwicket namespace compatibility tests."""

import importlib

import midwicket
import pypitch


def test_midwicket_metadata_matches_pypitch() -> None:
    assert midwicket.__version__ == pypitch.__version__
    assert midwicket.__author__ == pypitch.__author__


def test_midwicket_top_level_exports_remain_available() -> None:
    assert hasattr(midwicket, "express")
    assert hasattr(midwicket, "PyPitchSession")


def test_midwicket_documented_submodule_imports() -> None:
    modules = [
        "midwicket.express",
        "midwicket.api",
        "midwicket.data.loader",
        "midwicket.compute.winprob",
    ]

    for module_name in modules:
        loaded = importlib.import_module(module_name)
        assert loaded is not None
