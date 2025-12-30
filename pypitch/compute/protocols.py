from typing import Protocol, Union, List
import pyarrow as pa
import numpy as np

# A Metric is simply a function that takes an Arrow Table and returns a number.
class ScalarMetric(Protocol):
    def __call__(self, events: pa.Table) -> Union[float, int, None]:
        ...

class VectorMetric(Protocol):
    def __call__(self, events: pa.Table) -> Union[np.ndarray, List[float]]:
        ...
