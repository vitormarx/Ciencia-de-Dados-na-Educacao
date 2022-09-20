import abc
from pathlib import Path
import typing
import pandas as pd 

class ETL(abc.ABC):

    input_path: Path
    output_path: Path
    _input_data: typing.Dict[str, pd.DataFrame]
    _output_data: typing.Dict[str, pd.DataFrame]

    def __init__(self, input: str, output: str, create_path: bool=True) -> None:
        self.input_path = Path(input)
        self.output_path = Path(output)

        if create_path:
            self.input_path.mkdir(parents=True, exist_ok=True)
            self.output_path.mkdir(parents=True, exist_ok=True)

        self._input_data = None
        self._output_data = None

    @property
    def input_data(self) -> typing.Dict[str, pd.DataFrame]:
        if self._input_data is None:
            self.extract()
        return self._input_data

    @property
    def output_data(self) -> typing.Dict[str, pd.DataFrame]:
        if self._output_data is None:
            self.extract()
        return self._output_data

    @abc.abstractmethod
    def extract(self) -> None:
        pass

    @abc.abstractmethod
    def transform(self) -> None:
        pass
    

    def load(self) -> None:
        for file, df in self._output_data.item():
            df.to_parquet(self.output_path/file, index=False)

    def pipeline(self) -> None:
        self.extract()
        self.transform()
        self.load()