# Web stock to GCP data lake

Ingest web-sourced stocks into Google Cloud Platform (GCP) data lake.

## Table of Contents

- [Project Name](#project-name)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Features](#features)
  - [Installation](#installation)

## Description

The purpose of this data pipeline is to pull structured and semi-structured stock data from a website and place them in a GCP data lake so they may be used for longitudinal analyses.

## Features

  - Users can define which stocks are of interest within the 00_config.py application.
  - Raw data from web site is stored in Google Cloud Storage (GCS) as distinct objects. Data scraped from web site is only temporarily stored on the local machine. 
  - Transformed data ready for analytics is stored in Google BigQuery (GBQ).
  - Shell script available to schedule pipeline. At max, it can be run daily as data is only refreshed daily on source.

## Installation

Install requirements.txt in your environment. For reference, I used Python 3.11.

Set up a service account for the Google Cloud project you plan to use. This should have appropriate GCP roles/permissions to perform necessary interactions with GCS and GBQ. 

Create and store your .env file in your root directory. It should contain the following:
  - SRC_BASE_DIR ='path for storing your config.json'
  - TGT_BASE_DIR ='path for temporarily storing the stocks_YYYY-MM-DD.csv'
  - GCP_PROJECT_ID='GCP project id'
  - GOOGLE_APPLICATION_CREDENTIALS='Your service account credentials.json'
  - GCS_BUCKET='GCS bucket to store the data.'
  - LOCATION='Where you plan to store the data (e.g., US) in GCP'
