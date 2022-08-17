import boto3
import pandas as pd
import csv

base_url = "https://www.imdb.com/title/"

client = boto3.client('s3',
                        # Set up AWS credentials
                        aws_access_key_id="Secret",
                         aws_secret_access_key="Secret")

bucket_name = 'Secret'

object_key = 'imdb/imdb_id_list.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

# incomplete links
lambda_1 = csv_string.splitlines()

lambda_2 = []

with open(r'C:\Users\userpc\Desktop\lambda_2.csv') as file:
    reader = csv.reader(file)
    #total links other csv
    for i in reader:
        lambda_2.append(i[0])


lambda_list_1 = pd.DataFrame(
        {"tconst":lambda_1})
lambda_list_2 = pd.DataFrame(
        {"tconst":lambda_2})

cols = 'tconst'
new_links = pd.merge(lambda_list_1, lambda_list_2, on=cols, how ='outer', indicator=True)
new_links = new_links[new_links._merge != 'both']
new_links = new_links[new_links._merge == 'right_only']


new_links.to_csv(r"C:\Users\userpc\Desktop\Movies.csv")


