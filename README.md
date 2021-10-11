# Chatbot Concierge #

## About ##

A Dining Recommendation Chatbot deployed using AWS.
The website url is: http://dining.ai.agent.s3-website-us-east-1.amazonaws.com/

## Author ##
Wanqi, Wu (ww2578)
Chi, Wu (cw3326)

## Services ##

![image](https://user-images.githubusercontent.com/43989412/136854235-2a53af0f-ab25-4a1e-8a64-5f8fab229839.png)


1. Front-end is hosted using S3 bucket
2. API-gateway used for channeling back-end lambda functions
3. AWS LEX used to build and train the dining bot
4. Yelp APIs called to collect random restaurants from Manhattan area
5. DynamoDB used to store restaurant information
6. Opensearch Service instance created for indexing
7. (1) lambda function 0: used to call and send user utterances to AWS LEX service, and return LEX response to the user
   (2) lambda function 1: used to extract useful fields as well as validate the user utterances, send data to SQS
   (3) lambda function 2: used to retrieve recommendated restaurants information from Opensearch and DynamoDB, send data to SNS
8. SMS used to send recommendation results to users

## Example Message ##

![944ea0705dd5dc4db263daa703bbce5](https://user-images.githubusercontent.com/43989412/136856523-27be0a60-d70c-4eef-8718-b902248abbd7.jpg)
