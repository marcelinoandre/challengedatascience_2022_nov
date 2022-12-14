from pyspark.sql import DataFrame, functions as F
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, RegressionEvaluator


def printSeparator(symbol:str = '=', size:int = 50 ) -> None:
    print (symbol *size)


def get_count_columns_null_and_empty_values(df: DataFrame, vertical: bool = False) -> None:
    df.select(
        [
            F.count(
                F.when(
                    F.col(c).contains('None')
                    | F.col(c).contains('NULL')
                    | (F.col(c) == '')
                    | (F.col(c) == ' ')
                    | F.col(c).isNull()
                    | F.isnan(c), c
                )
            ).alias(c) for c in df.columns
        ]
    ).show(vertical=vertical)    




def get_metrics_regression_models(model_fit, df_teste):
    name_algoritmo = str(model_fit).split('Model:')[0]
    predict_test = model_fit.transform(df_teste)
    # Top5
    print( f'\n {name_algoritmo}', '*'*50)
    predict_test.select('label', 'prediction').orderBy('label', ascending=False).show(5)

    evaluator = RegressionEvaluator()
    return [
        ('R_2' , f'{evaluator.evaluate(predict_test, {evaluator.metricName: "r2"})}'),
        ('RMSE' , f'{evaluator.evaluate(predict_test, {evaluator.metricName: "rmse"})}'),
        ('Algorítimo' , f'{name_algoritmo}')
    ]     