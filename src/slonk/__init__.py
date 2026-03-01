from pathlib import Path
from typing import Any, Iterable, Optional, Protocol, Union, runtime_checkable
from urllib.parse import urlparse
import subprocess

import cloudpathlib
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker


class Base(DeclarativeBase):
    pass


class ExampleModel(Base):
    __tablename__ = "example"
    id = Column(String, primary_key=True)
    data = Column(String)


@runtime_checkable
class Handler(Protocol):
    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]: ...


# Define handlers
class CloudPathHandler:
    def __init__(self, url: str) -> None:
        self.url = url
        self.cloud_path = cloudpathlib.CloudPath(url)

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        if input_data is not None:
            self.write(input_data)  # Write input data to the cloud path
            return input_data  # Return the input data for onward processing
        else:
            return self.read()  # Read from cloud path if no input data

    def write(self, data: Iterable[str]) -> None:
        with self.cloud_path.open("w") as file:
            for line in data:
                file.write(line + "\n")

    def read(self) -> Iterable[str]:
        with self.cloud_path.open("r") as file:
            return file.readlines()


class LocalPathHandler:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        if input_data is not None:
            self.write(input_data)  # Write input data to the local path
            return input_data  # Return the input data for onward processing
        else:
            return self.read()  # Read from local path if no input data

    def write(self, data: Iterable[str]) -> None:
        with self.path.open("w") as file:
            for line in data:
                file.write(line + "\n")

    def read(self) -> Iterable[str]:
        with self.path.open("r") as file:
            return file.readlines()


class ShellCommandHandler:
    def __init__(self, command: str) -> None:
        self.command = command

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        if input_data is not None:
            input_string = "\n".join(input_data)
            return [self._run_command(input_string)]
        else:
            return []

    def _run_command(self, input_string: str) -> str:
        process = subprocess.Popen(
            self.command,
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate(input=input_string.encode())
        if process.returncode != 0:
            raise RuntimeError(f"Command failed with error: {stderr.decode()}")
        return stdout.decode().strip()


class SQLAlchemyHandler:
    def __init__(
        self, model: type[Base], session_factory: sessionmaker[Session]
    ) -> None:
        self.model = model
        self.session_factory = session_factory

    def process(self, input_data: Optional[Iterable[Any]]) -> Iterable[str]:
        session = self.session_factory()
        try:
            records = session.query(self.model).all()
            return [f"{record.id}\t{record.data}" for record in records]
        finally:
            session.close()


StageType = Union[Handler, "Slonk"]


class Slonk:
    def __init__(self, session_factory: Optional[sessionmaker[Session]] = None) -> None:
        self.stages: list[StageType] = []
        self.session_factory = session_factory

    def __or__(
        self,
        other: Union[str, "Slonk", type[Base], Any],
    ) -> "Slonk":
        if isinstance(other, str):
            if self._is_local_path(other):
                self.stages.append(LocalPathHandler(other))
            elif self._is_cloud_path(other):
                self.stages.append(CloudPathHandler(other))
            else:
                self.stages.append(ShellCommandHandler(other))
        elif isinstance(other, Slonk):
            self.stages.append(other)
        elif isinstance(other, type) and issubclass(other, Base):
            if self.session_factory is None:
                raise ValueError(
                    "Cannot use SQLAlchemy models without a session_factory. "
                    "Pass session_factory to Slonk()."
                )
            self.stages.append(SQLAlchemyHandler(other, self.session_factory))
        elif callable(other):
            self.stages.append(_CallableHandler(other))
        else:
            raise TypeError(f"Unsupported type: {type(other)}")
        return self

    def run(self, input_data: Optional[Iterable[Any]] = None) -> Iterable[str]:
        output: Any = input_data
        for stage in self.stages:
            if isinstance(stage, Slonk):
                output = stage.run(output)
            else:
                output = stage.process(output)
        return output if output is not None else []

    def _is_local_path(self, string: str) -> bool:
        return (
            string.startswith("/")
            or string.startswith("./")
            or string.startswith("../")
            or string.startswith("file://")
        )

    def _is_cloud_path(self, string: str) -> bool:
        parsed_url = urlparse(string)
        return parsed_url.scheme in ("s3", "gs", "azure", "wasb")

    def tee(self, pipeline: "Slonk") -> "Slonk":
        tee_stage = TeeHandler(pipeline)
        self.stages.append(tee_stage)
        return self


class _CallableHandler:
    """Wraps a plain callable so it conforms to the Handler protocol."""

    def __init__(self, func: Any) -> None:
        self.func = func

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        return self.func(input_data)


class TeeHandler:
    def __init__(self, pipeline: Slonk) -> None:
        self.pipeline = pipeline

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        results: list[str] = []
        if input_data is not None:
            # Pass through the input data unchanged
            results.extend(input_data)
            # Also run the tee pipeline
            results.extend(self.pipeline.run(input_data))
        return results


# Helper function for creating a tee stage
def tee(pipeline: Slonk) -> Slonk:
    s = Slonk()
    s.tee(pipeline)
    return s


# Example usage
if __name__ == "__main__":
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine)

    # Add sample records to ExampleModel
    session = SessionLocal()
    session.add_all(
        [
            ExampleModel(id="1", data="Hello World"),
            ExampleModel(id="2", data="Goodbye World"),
            ExampleModel(id="3", data="Hello Again"),
        ]
    )
    session.commit()

    # Create a pipeline
    pipeline = (
        Slonk(session_factory=SessionLocal)
        | ExampleModel  # Automatically wraps ExampleModel with SQLAlchemyHandler
        | "grep Hello"  # Shell command to filter records
        | tee(
            Slonk() | "./file.csv"  # Tee to a local path
        )  # Forks pipeline to handle both destinations
        | "s3://my-bucket/my-file.txt"  # Tee to a cloud path
    )

    # Run pipeline
    result = pipeline.run()
    print("Pipeline result:")
    print("\n".join(result))
