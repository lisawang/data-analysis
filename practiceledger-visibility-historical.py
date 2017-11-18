# -*- coding: utf-8 -*-

# In[15]:
import json
from datetime import datetime
import logging
import re
import tempfile
import uuid
import os

import boto3
import pandas as pd
import sqlalchemy
from io import BytesIO
import gzip

# ## Setup

s3 = boto3.resource('s3')
client = boto3.client('s3')

def iterate_bucket_items(client, bucket, prefix='', accept_fn=None):
    """
    Generator that iterates over all objects in a given s3 bucket

    Inputs:
     - client: AWS S3 client
     - bucket: S3 bucket name
     - prefix: filename/folder key prefix
     - accept_fn: function to filter returned filenames

    See http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.list_objects_v2
    for return data format
    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    """
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        for item in page['Contents']:
            if accept_fn is not None and accept_fn(item['Key']) == True:
                yield item
            else:
                pass

def get_local_filename(key):
    return key.split('/')[-1]

def get_date_from_filename(key):
    filename = get_local_filename(key)
    return '{0}-{1}-{2}'.format(filename[:4], filename[4:6], filename[6:8])

# # Test the functions
    # print(get_local_filename('processed/0.csv'))
    # print(get_date_from_filename('processed/20171029042718AEDT_PL_FULL_EXTRACT.csv'))



if __name__ == '__main__':
# ## Main script
#
# This script will iterate through files in the S3 bucket that end with `...PL_FULL_EXTRACT.csv`.
#
# It will keep track of two files: `yesterday` and `today`. Both files are downloaded and loaded into Pandas dataframes - a 'dataframe' is what Pandas calls a table. The two tables are joined (called `merge` in Pandas) and then filtered to find rows where the visibility was 'True' yesterday and 'False' today.
#
# The rows are output to the file `visibility-changed.csv`.

    yesterday_filename = None
    today_filename = None
    with open('visibility-changed.csv', 'w') as fp:

        fp.write('id,date_visibility_changed\n')

        for f in iterate_bucket_items(client, bucket='practiceledger-droplet-dev', prefix='processed/',
                                      accept_fn=lambda key: re.match('.*PL_FULL_EXTRACT.csv', key) is not None):
            ## Bucket items are returned in order: https://stackoverflow.com/questions/4102115/does-the-listbucket-command-guarantee-the-results-are-sorted-by-key
            #print(f)

            if yesterday_filename is None:
                # Download initial yesterday file
                yesterday_filename = f['Key']
                s3.Bucket('practiceledger-droplet-dev').download_file(yesterday_filename, './' + get_local_filename(yesterday_filename))
                df_yesterday = pd.read_csv(get_local_filename(yesterday_filename))
                continue

            # Download today file
            today_filename = f['Key']
            s3.Bucket('practiceledger-droplet-dev').download_file(today_filename, './' + get_local_filename(today_filename))
            df_today = pd.read_csv(get_local_filename(today_filename))

            print('Comparing {0} and {1}'.format(get_date_from_filename(yesterday_filename),
                                                 get_date_from_filename(today_filename)))

            # Find rows where visiblity changed
            df_merged = pd.merge(df_yesterday[['id', 'visibility']],
                             df_today[['id', 'visibility']],
                            on='id',
                            suffixes=('_yesterday', '_today'))

            # Write rows to output CSV
            visibility_changed = df_merged[(df_merged['visibility_yesterday'] == 't')
                                           & (df_merged['visibility_today'] == 'f')]
            for row in visibility_changed.iterrows():
                fp.write('{0},{1}\n'.format(row[1].id, get_date_from_filename(yesterday_filename)))

            # Delete yesterday file
            try:
                os.remove(get_local_filename(yesterday_filename))
            except:
                print('Error removing file ' + get_local_filename(yesterday_filename))

            #Swap yesterday and today filenames/dataframes
            yesterday_filename = today_filename
            today_filename = None
            df_yesterday = df_today

        # Delete final file
        try:
            os.remove(get_local_filename(yesterday_filename))
        except:
            print('Error removing file ' + get_local_filename(yesterday_filename))


# # ## Testing

# # Download yesterday file
# s3.Bucket('practiceledger-droplet-dev').download_file('processed/20171029042718AEDT_PL_FULL_EXTRACT.csv',
#                                                       './' + get_local_filename('processed/20171029042718AEDT_PL_FULL_EXTRACT.csv'))
# df_yesterday = pd.read_csv(get_local_filename('processed/20171029042718AEDT_PL_FULL_EXTRACT.csv'))

# # Download today file
# s3.Bucket('practiceledger-droplet-dev').download_file('processed/20171030042718AEDT_PL_FULL_EXTRACT.csv',
#                                                       './' + get_local_filename('processed/20171030042718AEDT_PL_FULL_EXTRACT.csv'))
# df_today = pd.read_csv(get_local_filename('processed/20171030042718AEDT_PL_FULL_EXTRACT.csv'))


