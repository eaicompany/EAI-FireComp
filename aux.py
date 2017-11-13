import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os
import json
import cv2
import numpy as np
import pandas
import contextlib

BUCKET_NAME = 'sentinel-s2-l1c'
DATA_FOLDER = 'DATA'
client = boto3.client('s3', config=Config(signature_version=UNSIGNED))


def download_file(key):
    key_dir, key_filename = os.path.split(key)
    out_dir = os.path.join(DATA_FOLDER, key_dir)
    out_filename = os.path.join(out_dir, key_filename)
    if not os.path.exists(out_filename):
        client.download_file(BUCKET_NAME, key, out_filename)


def search_fire_b12(coverage_percentage=100, cloudy_percentage=40, fire_threshold=90, fire_pixels_pct= 0.03):
    # Look in the metadata for images to download

    data = []
    for root, dirs, files in os.walk("DATA"):
        for file in files:
            if file == 'tileInfo.json':
                filename = os.path.join(root, file)
                with open(filename, 'r') as json_file:
                    data.append(json.load(json_file))

    data = pandas.DataFrame(data)
    data.timestamp = pandas.to_datetime(data.timestamp)
    data = data.sort_values('timestamp', ascending=True)

    # Filtering tiles by data coverage
    mask = data['dataCoveragePercentage'] >= coverage_percentage
    data_cov = data.loc[mask, :]

    # Filtering tiles by pixel percentage
    mask2 = data_cov['cloudyPixelPercentage'] <= cloudy_percentage
    data_cov_cloud = data_cov.loc[mask2, :]
    data_cov_cloud.to_csv('data_cov_cloud.csv')

    # Filtering tiles by datetime
    mask3 = (data_cov_cloud.timestamp > '2015-06-01') & (data_cov_cloud.timestamp < '2015-10-21') | (data_cov_cloud.timestamp > '2016-06-01') & (data_cov_cloud.timestamp < '2016-10-21') | (
                                                                                                    data_cov_cloud.timestamp > '2017-06-01') & (
                                                                                                    data_cov_cloud.timestamp < '2017-10-21')
    data_cov_cloud_months = data_cov_cloud.loc[mask3, :]
    data_cov_cloud_months.to_csv('data_cov_cloud_months.csv')

    print(len(data_cov_cloud_months), 'images to download.')

    DATA_PATH = 'DATA'

    fire_records = open("fire_records.csv", "w")
    fire_records.write('timestamp,path\n')
    fire_count = 0
    min_b12 = []

    for i in range(len(data_cov_cloud_months)):
        path_prev = data_cov_cloud_months.iloc[i, 5]

        path = os.path.join(data_cov_cloud_months.iloc[i, 5], 'preview')

        download_file(os.path.join(path, 'B12.jp2'))
        download_file(os.path.join(path, 'B11.jp2'))

        path = os.path.join(DATA_PATH, path)

        image_thumb = cv2.imread(os.path.join(path, 'B12.jp2'), cv2.IMREAD_GRAYSCALE)

        min_b12.append(np.min(image_thumb))

        if np.max(image_thumb) >= fire_threshold:

            thresh = cv2.threshold(image_thumb, fire_threshold, 255, cv2.THRESH_BINARY)[1]

            thresh = cv2.dilate(thresh, None, iterations=4)
            thresh = cv2.erode(thresh, None, iterations=2)

            numPixels = cv2.countNonZero(thresh)

            if numPixels > image_thumb.shape[1] * fire_pixels_pct:
                # identify as fire and store the preview.jpg locations for visual confirmation
                fire_count += 1
                fire_records.write(
                    str(data_cov_cloud_months.iloc[i, 11]) + ',' + os.path.join(path_prev, 'preview.jpg\n'))
                download_file(os.path.join(path_prev, 'preview.jpg'))

    print('Found', fire_count, 'tiles possibly containing a fire.')
    fire_records.close()
    return min_b12


if __name__ == '__main__':
    search_fire_b12()
