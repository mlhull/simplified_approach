# Project Name

Ingest web-sourced stocks into GCP data lake

## Table of Contents

- [Project Name](#project-name)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Features](#features)
  - [Installation](#installation)

## Description

The purpose of this data pipeline is to pull structured and semi-structued stock data from a website and place them in a Google Cloud Platform (GCP) data lake so they may be used for longitudinal analyses.

## Features

  - Users can define which stocks are of interest within the 00_config.py application.
  - Raw data from website is stored in Google Cloud Storage (GCS) as distinct objects. Data scraped from website is only temporarily stored on the local machine. 
  - Transformed data ready for analytics is stored in Google BigQuery (GBQ)
  - Shell script available to schedule pipeline. At max, it can be run daily as data is only refreshed daily on source

## Installation

Install requirements.txt in your environment. For reference, I use python 3.11.

Set up a service account for the Google Cloud project you plan to use. This should have permissions to perform necessary interactions with GCS and GBQ. 

Create and store your .env file in your root directory. It should contain the following:
  - SRC_BASE_DIR ='path for storing your config.json'
  - TGT_BASE_DIR ='path for temporarily storing the stocks_YYYY-MM-DD.csv'
  - GCP_PROJECT_ID='GCP project id'
  - GOOGLE_APPLICATION_CREDENTIALS='Your service account credentials.json'
  - GCS_BUCKET='GCS bucket to store the data.'
  - LOCATION='Where you place to store the data (e.g., US)'