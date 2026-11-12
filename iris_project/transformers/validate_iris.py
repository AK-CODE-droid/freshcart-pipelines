if 'transformer' not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except (ImportError, ModuleNotFoundError):
        def transformer(fn):
            return fn

if 'test' not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except (ImportError, ModuleNotFoundError):
        def test(fn):
            return fn


@transformer
def transform(df, *args, **kwargs):
    return df.copy()


@test
def test_iris_data(output, *args):
    assert output.isnull().sum().sum() == 0, 'Missing values found in Iris dataset'
    assert output['target'].nunique() == 3, 'Iris must have exactly 3 classes'

