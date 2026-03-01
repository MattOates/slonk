"""Shared test helpers — ExampleBase / ExampleModel and other fixtures used across test modules."""

from __future__ import annotations

from sqlalchemy import Column, String
from sqlalchemy.orm import DeclarativeBase


class ExampleBase(DeclarativeBase):
    pass


class ExampleModel(ExampleBase):
    __tablename__ = "example"
    id = Column(String, primary_key=True)
    data = Column(String)
