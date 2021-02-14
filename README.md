# Twitter streaming collection



## Introduction

In this project I implemented a data pipeline to stream real-time Twitter tweets. I used tweep, a python library, to access to Twitter streaming API and Kafka as my distributed queuing service. For streaming processing I choosed Spark and the finally the filtered and stored data will be stored in MongoDB.



## Developement Environments

In order to access to [Twitter Streaming API ,](https://developer.twitter.com/en/docs/tutorials/consuming-streaming-data) we  need to apply our own four keys: Consumer Key, Consumer Secrect, Access Token and Access Token Secret via https://dev.twitter.com/apps/new, more information about it can be found from [Twitter Developer Site](https://developer.twitter.com/en/docs/authentication/oauth-1-0a/obtaining-user-access-tokens) .

There are some tools need to be installed before running the project:

##### 1. mongoDB

Download mongoDB from [mongoDB compass](https://www.mongodb.com/try/download/compass), and follow the installation instruction on https://docs.mongodb.com/manual/installation/, assume that you create your mongoDB's dbpath at /usr/local/var/mongodb and logpath at /usr/local/var/log/, then you can run the command in your terminal to start it:

$ mongod --dbpath /usr/local/var/mongodb --logpath /usr/local/var/log/mongodb/mongo.log --fork





I choose tweepy as my 



as my  and .I perfomed a basic key words filter of real-time tweets and store 



1)flatten JSON

JSON may be the most popular data-interchange format over the whole internet, however the nested JSON might have very complex structure and not so firendly for structured schema. I choose [json-flattener](https://github.com/wnameless/json-flattener) to flatten the JSON format object before storing into database.

2) MongoDB

The information about java dependencies and related versions can be found in the pom file.
