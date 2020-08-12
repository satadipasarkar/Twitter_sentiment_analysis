# TwitterSentimentAnalysis

### Goal
We want to do sentiment analysis on the various tweets for certain global events of importance in the past

### Flow
1. The input compressed files are decompressed and uploaded to AWS S3 to be processed by downstream systems.
2. _tweet_ids_ are read from the S3 bucket and corresponding tweets are fetched from Twitter API.
3. The _tweet_ids_ along with the tweets are stored in RDS.
4. The tweets are read from RDS and AWS Comprehend is used to derive the sentiments for the tweets.
5. _tweet_ids_ along with the sentiment are stored in RDS.
6. The sentiments are read from RDS and transformed into the required output data model- connected to Tableau.
7. Tableau gets the data from RDS by connetcing directly with the PostgreSQL Server.


### Architecture Diagram

![alt text](https://raw.githubusercontent.com/satadipasarkar/Twitter_sentiment_analysis_big_data_project/master/assets/architecture_diagram_twitter_sentiment.svg.png)

