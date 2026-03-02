"""Shared test helpers — TestBase / TestModel and other fixtures used across test modules."""

from __future__ import annotations

from sqlalchemy import Column, String
from sqlalchemy.orm import DeclarativeBase


class TestBase(DeclarativeBase):
    __test__ = False


class TestModel(TestBase):
    __test__ = False
    __tablename__ = "example"
    id = Column(String, primary_key=True)
    data = Column(String)
