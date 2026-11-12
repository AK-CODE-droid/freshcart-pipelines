if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except (ImportError, ModuleNotFoundError):
        def data_exporter(fn):
            return fn


@data_exporter
def export_data(df, *args, **kwargs):
    df.to_csv('iris_predictions.csv', index=False)
    print('Saved', len(df), 'predictions to iris_predictions.csv')

