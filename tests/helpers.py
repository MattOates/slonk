"""Shared test helpers — _TestBase / _TestModel and other fixtures used across test modules."""

from __future__ import annotations

from sqlalchemy import Column, String
from sqlalchemy.orm import DeclarativeBase


class _TestBase(DeclarativeBase):
    pass


class _TestModel(_TestBase):
    __tablename__ = "example"
    id = Column(String, primary_key=True)
    data = Column(String)


# Public aliases (non-underscore) for use in tests.
TestBase = _TestBase
TestModel = _TestModel
