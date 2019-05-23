import os
import pandas
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.recommendation import ALS
from pyspark.ml.feature import StringIndexer
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    """An amazon kindle recommendation engine"""
    
    def __count_and_average_ratings(self):
        """Updates the movies ratings counts from 
        the current data self.ratings
        """
        logger.info("Counting kindle ratings...")
        self.rating_count= self.datas.count()
 
    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="item_id", ratingCol="rating",
                         coldStartStrategy="drop")
        self.model = self.als.fit(self.column_trained)
        logger.info("ALS model built!")
        
    def __predict_ratings(self, x_df):
        """Gets predictions for a given (userID, itemID) formatted RDD
        """
        predicted = self.model.transform(x_df)
        return predicted
    
    def get_rec_user(self, count):
        """Add additional kindle ratings in the format (user_id, item_id, rating)
        """
        ratings=self.model.recommendForAllItems(count)
        
    def get_top_ratings(self, kindle_count):
        """Recommends up to kindle_count top unrated kindle to user_id
        """
        logger.info("Generating rec kindle for all user")
        ratings = self.model.recommendForAllUsers(kindle_count)

        return ratings.toJSON().collect()
    
    def get_ratings_for_item_id(self, user_id, item_id):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """
        requested_item_RDD = self.sc.parallelize(item_id).map(lambda x: (user_id, x))
        # Get predicted ratings
        ratings = self.__predict_ratings(requested_item_RDD.toDF(['user_id','item_id'])).toJSON().collect()
        return ratings
 

    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
 
        logger.info("Starting up the Recommendation Engine: ")
 
        self.spark_session = spark_session
 
        # Load kindle data for later use
        logger.info("Loading Kindle data...%s",os.getcwd())

        data_file_path = os.path.join(os.getcwd(), 'kindle-reviews.csv')
        df = spark_session.read.csv(data_file_path, header=True, inferSchema=True).na.drop()
        
        stringindexer = StringIndexer(inputCol='reviewerID',outputCol='reviewerID_index')
        stringindexer.setHandleInvalid("keep")
        model = stringindexer.fit(df)
        indexed = model.transform(df)
        #self.uid_indexer = model

        stringindexer_item = StringIndexer(inputCol='asin',outputCol='asin_index')
        stringindexer_item.setHandleInvalid("keep") 
        model = stringindexer_item.fit(indexed)
        indexed = model.transform(indexed)
        #self.iid_indexer = model

        self.datas=df
        self.column_trained=indexed.selectExpr(['reviwerID_index as user_id','asin_index as item_id','overall as rating'])
        
        # Pre-calculate kindle ratings counts
        self.__count_and_average_ratings()
 
        # Train the model
        self.__train_model()