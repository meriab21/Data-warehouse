# Speech_to_text_data_pipeline

![image](https://user-images.githubusercontent.com/44437166/178087878-3434a31a-a0c9-4078-a530-a59f5ac5307b.png)

**Table of content**

- [Overview](#overview)
- [Install](#install)
- [Data](#data)
- [Folders](#notebooks)

## Overview

> The purpose of this week’s challenge is to build a data engineering pipeline that allows recording millions of Amharic and Swahili speakers reading digital texts in-app and web platforms. There are a number of large text corpora we will use
> We will design and build a robust, large scale, fault tolerant, highly available Kafka cluster that can be used to post a sentence and receive an audio file. By the end of this project, we will produce a tool that can be deployed to process posting and receiving text and audio files from and into a data lake, apply transformation in a distributed manner, and load it into a warehouse in a suitable format to train a speech-t0-text model. 
## Install

```
git clone https://github.com/Reiten-10Academy/Speech_to_text_data_pipeline
cd Speech_to_text_data_pipeline
pip install -r requirements.txt
```

## Data

Data can be found [here](https://github.com/IsraelAbebe/An-Amharic-News-Text-classification-Dataset)

## Pipeline
> flow of data is shown with the arrows, and the order of execution is shown with the numbers attached to the bottom of the arrows.

![image](https://user-images.githubusercontent.com/44437166/178345167-e5573b64-e064-4324-b080-a243e5857469.png)

> - 1: Load original dataset as a csv to from unprocessed folder cleaning and selecting script to be processed by spark
> - 2: Load a csv file containing id and text column to interim folder from cleaning script
> - 3: Load cleaned data set from interim folder in s3 bucket to producer script that sends one row of data every X seconds to kafka topic  
> - 4: Send one row of data (sentence and Id) to kafka every X seconds.
> - 5: Request for a sentence is sent out to a react frontend
> - 6: The GET request is transfered from the react frontend to flask api
> - 7: A kafka consumer requests to load latest sentence added to kafka topic
> - 8: A kafka Topic responds back by sending a sentence and its id to the consumer
> - 9: A flask api responds to the GET request and sends the sentence and id to the react frontend
> - 10: The react frontend sends the loaded sentence to the user screen
> - 11: The User screen records an audio and sends the audio along with the sentence to the react frontend
> - 12: The react frontend sends a POST request to the flask api by putting the sentence, id, and audio as a body of the message
> - 13: The flask api will rename the audio with the sentence id and sends it to an s3 bucket and put it in "unprocessed" folder and creates a new column of data that holds the url of this audio file besides the sentence and id column. Finally, it sends this metadata to a kafka topic through a kafka producer.
> - 14: A kafak topic sends rows containing id, sentence, and URL information to a text loader script that holds a kafka consumer.
> - 15: A text loader script will consume all rows of data in kafka and put them in the interim folder
> - 16: A audio cleaner script that runs a pyspark code will load audios and meta data from the unprocessed folder and the interim folder respectively and performs some final cleaning and preparation
> - 17: The Audio cleaner script will move the cleaned audio to a folder called "audio" inside the "processed" folder and put the metadata containing information about the audio file in the "processed" folder

#### description

    Building a 

## folders
<!-- 
> - backend: a flask server and a bunch of python scripts that process data in pipeline 
> - frontend: a react application.
> - extra: contains, notebooks, docs, and other development and testing files. -->

## Author

- 👤 **Meron Abate**




## Show your support

Give a ⭐ if you like this project!
