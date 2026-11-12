import pandas as pd
from sklearn.datasets import load_iris

if 'data_loader' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except (ImportError, ModuleNotFoundError):
        def data_loader(fn):
            return fn

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except (ImportError, ModuleNotFoundError):
        def test(fn):
            return fn


@data_loader
def load_data(*args, **kwargs):
    iris = load_iris(as_frame=True)
    df = iris.frame                      # 4 feature columns + 'target'
    print('Rows loaded:', len(df))
    return df


@test
def test_output(output, *args):
    assert len(output) == 150, 'Iris must have 150 rows'
