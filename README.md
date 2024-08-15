# slonk

Experiment in typed pipelining of data between python, shell and various data sources and sinks.

```python

# Create a pipeline
pipeline = (
    Slonk()
    | ExampleModel  # Automatically wraps ExampleModel with SQLAlchemyHandler, does this by noticing DeclarativeBase type
    | "grep Hello"  # Shell command to filter records, shelling out assumed on non path strings, otherwise IO assumed and sink context anywhere other than the head of the pipeline
    | tee("./file.csv")  # Tee to a local path, but could be anything
    # Forks pipeline to handle both destinations
    | "s3://my-bucket/my-file.txt"  # Tee to a cloud path
    )
```
