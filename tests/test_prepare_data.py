from sklearn.datasets import load_iris
from iris_project.transformers.prepare_data import transform
from iris_project.transformers.validate_iris import transform as validate_transform, test_iris_data


def test_split_is_80_20():
    df = load_iris(as_frame=True).frame
    out = transform(df)
    assert (out['split'] == 'train').sum() == 120
    assert (out['split'] == 'test').sum() == 30


def test_validate_iris_block():
    df = load_iris(as_frame=True).frame
    out = validate_transform(df)
    test_iris_data(out)
    assert len(out) == 150
    assert out.isnull().sum().sum() == 0
    assert out['target'].nunique() == 3
