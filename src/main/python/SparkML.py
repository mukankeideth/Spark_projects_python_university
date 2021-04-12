from pyspark.ml import Pipeline
import pyspark.ml.classification as CL
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
import sys


def connect_four_classifier(path):
    spark = SparkSession \
        .builder \
        .appName("Connect 4 classifier") \
        .getOrCreate()

    data = spark \
        .read \
        .format("libsvm") \
        .option("numFeatures", 126) \
        .load(path)
    data.show()

    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabels").fit(data)

    featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=10)

    (training_data, test_data) = data.randomSplit([0.7, 0.3])

    #lsvc = CL.MultilayerPerceptronClassifier(maxIter=100, labelCol="indexedLabels",
    #                                         featuresCol="indexedFeatures")

    dt = CL.DecisionTreeClassifier(labelCol="indexedLabels", featuresCol="indexedFeatures")

    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

    #layer1 = [126, 100, 50, 3]
    #layer2 = [126, 50, 20, 10, 3]
    #layer3 = [126, 75, 100, 120, 3]
    #layer4 = [126, 300, 3]

    #layerArr = [layer1, layer2, layer3, layer4]

    paramGrid = ParamGridBuilder() \
        .addGrid(dt.maxDepth, [5, 10, 15])\
        .addGrid(dt.maxBins, [16, 32, 64]).build()

    test = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(labelCol="indexedLabels",
                                                                      predictionCol="prediction",
                                                                      metricName="accuracy"), numFolds=10)

    cvModel = test.fit(training_data)

    prediction = cvModel.transform(test_data)

    selected = prediction.select("prediction", "indexedLabels", "features").show(5)

    #####
    # model = pipeline.fit(training_data)

    # prediction = model.transform(test_data)

    # prediction.select("prediction", "indexedLabels", "features").show(5)

    evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabels", predictionCol="prediction",
                                                  metricName="accuracy")

    accuracy = evaluator.evaluate(prediction)
    print("Test Error = %g " % (1.0 - accuracy))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python SparkML <Dataset path>")
        path = "./LIBSVM/connect-4"
    else:
        connect_four_classifier(sys.argv[1])
