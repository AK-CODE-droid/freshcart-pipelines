import joblib
from sklearn.tree import DecisionTreeClassifier

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

FEATURES = ['sepal length (cm)', 'sepal width (cm)',
            'petal length (cm)', 'petal width (cm)']


@transformer
def train(df, *args, **kwargs):
    max_depth = int(kwargs.get('max_depth', 3))

    train_df = df[df['split'] == 'train']
    test_df = df[df['split'] == 'test'].copy()

    model = DecisionTreeClassifier(max_depth=max_depth, random_state=42)
    model.fit(train_df[FEATURES], train_df['target'])

    test_df['predicted'] = model.predict(test_df[FEATURES])
    accuracy = (test_df['predicted'] == test_df['target']).mean()
    print('max_depth =', max_depth, '| Test accuracy =', round(accuracy, 3))

    joblib.dump(model, 'iris_model.pkl')
    return test_df


@test
def test_accuracy(output, *args):
    accuracy = (output['predicted'] == output['target']).mean()
    assert accuracy >= 0.8, 'Model accuracy is below 80%'
