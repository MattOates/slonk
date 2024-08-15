from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union, Callable, Type
from urllib.parse import urlparse
import subprocess
import cloudpathlib  # For handling cloud paths
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
engine = create_engine('sqlite:///:memory:')  # Use an in-memory SQLite database
Session = sessionmaker(bind=engine)

class ExampleModel(Base):
    __tablename__ = 'example'
    id = Column(String, primary_key=True)
    data = Column(String)

Base.metadata.create_all(engine)

# Define handlers
class CloudPathHandler:
    def __init__(self, url: str):
        self.url = url
        self.cloud_path = cloudpathlib.CloudPath(url)

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        if input_data is not None:
            self.write(input_data)  # Write input data to the cloud path
            return input_data  # Return the input data for onward processing
        else:
            return self.read()  # Read from cloud path if no input data

    def write(self, data: Iterable[str]):
        with self.cloud_path.open('w') as file:
            for line in data:
                file.write(line + '\n')

    def read(self) -> Iterable[str]:
        with self.cloud_path.open('r') as file:
            return file.readlines()

class LocalPathHandler:
    def __init__(self, path: str):
        self.path = Path(path)

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        if input_data is not None:
            self.write(input_data)  # Write input data to the local path
            return input_data  # Return the input data for onward processing
        else:
            return self.read()  # Read from local path if no input data

    def write(self, data: Iterable[str]):
        with self.path.open('w') as file:
            for line in data:
                file.write(line + '\n')

    def read(self) -> Iterable[str]:
        with self.path.open('r') as file:
            return file.readlines()

class ShellCommandHandler:
    def __init__(self, command: str):
        self.command = command

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        if input_data is not None:
            input_string = "\n".join(input_data)
            return [self._run_command(input_string)]
        else:
            return []

    def _run_command(self, input_string: str) -> str:
        process = subprocess.Popen(
            self.command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(input=input_string.encode())
        if process.returncode != 0:
            raise RuntimeError(f"Command failed with error: {stderr.decode()}")
        return stdout.decode().strip()

class SQLAlchemyHandler:
    def __init__(self, model: Base):
        self.model = model

    def process(self, input_data: Optional[Iterable[Any]]) -> Iterable[str]:
        session = Session()
        records = session.query(self.model).all()
        session.close()
        return [f"{record.id}\t{record.data}" for record in records]

class Slonk:
    def __init__(self):
        self.stages = []

    def __or__(self, other: Union[str, 'Slonk', Callable[[Iterable[str]], Iterable[str]]]):
        if isinstance(other, str):
            if self._is_local_path(other):
                self.stages.append(LocalPathHandler(other))
            elif self._is_cloud_path(other):
                self.stages.append(CloudPathHandler(other))
            else:
                self.stages.append(ShellCommandHandler(other))
        elif isinstance(other, Slonk):
            self.stages.append(other)
        elif isinstance(other, Callable):
            if hasattr(other, '__annotations__'):
                input_type = list(other.__annotations__.values())[0]
                if issubclass(input_type, Base):
                    self.stages.append(SQLAlchemyHandler(other))
                else:
                    self.stages.append(other)
            else:
                self.stages.append(other)
        else:
            raise TypeError(f"Unsupported type: {type(other)}")
        return self

    def run(self, input_data: Optional[Iterable[Any]] = None) -> Iterable[str]:
        output = input_data
        for stage in self.stages:
            output = stage.process(output)  # Pass the output to each stage's process method
        return output

    def _is_local_path(self, string: str) -> bool:
        return string.startswith('/') or string.startswith('./') or string.startswith('file://')

    def _is_cloud_path(self, string: str) -> bool:
        parsed_url = urlparse(string)
        return parsed_url.scheme in ('s3', 'gs', 'azure', 'wasb')

    def tee(self, pipeline: 'Slonk') -> 'Slonk':
        new_pipeline = Slonk()
        new_pipeline.stages.extend(pipeline.stages)  # Fork the stages
        tee_stage = TeeHandler(pipeline)
        self.stages.append(tee_stage)
        return self

class TeeHandler:
    def __init__(self, pipeline: Slonk):
        self.pipeline = pipeline

    def process(self, input_data: Optional[Iterable[str]]) -> Iterable[str]:
        results = []
        if input_data is not None:
            # Process the current pipeline stages
            results.extend(Slonk().run(input_data))
            # Process the tee pipeline
            results.extend(self.pipeline.run(input_data))
        return results

# Helper function for creating a tee stage
def tee(pipeline: Slonk) -> Slonk:
    return Slonk().tee(pipeline)

# Example usage
if __name__ == "__main__":
    # Add sample records to ExampleModel
    session = Session()
    session.add_all([
        ExampleModel(id="1", data="Hello World"),
        ExampleModel(id="2", data="Goodbye World"),
        ExampleModel(id="3", data="Hello Again")
    ])
    session.commit()

    # Create a pipeline
    pipeline = (Slonk()
                | ExampleModel  # Automatically wraps ExampleModel with SQLAlchemyHandler
                | "grep Hello"  # Shell command to filter records
                | tee(Slonk()
                      | "./file.csv"  # Tee to a local path
                     )  # Forks pipeline to handle both destinations
                | "s3://my-bucket/my-file.txt"  # Tee to a cloud path
    )

    # Run pipeline
    result = pipeline.run()
    print("Pipeline result:")
    print('\n'.join(result))
