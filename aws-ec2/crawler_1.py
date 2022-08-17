import boto3
from webCrawlerIMDB import get_all_information
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


base_url = "https://www.imdb.com/title/"

client = boto3.client('s3',
                        # Set up AWS credentials
                        aws_access_key_id="secret",
                         aws_secret_access_key="secret")

bucket_name = 'secret'

object_key = 'imdb/imdb_id_list.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

# incomplete links
mergeString = csv_string.splitlines()
complete_url = []

# complete links
for text in mergeString:
    complete_url.append(base_url + text + "/")

s3 = boto3.client('s3')

# create folder
s3.put_object(Bucket='read-and-upload', Key=('IMDB_list'+'/'))
# first element not complete url
del complete_url[0]
all_information = []
new_list = []

for i in range(0, 75000):
    new_list.append(complete_url[i])

# complete_url -> new_list

if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=4) as p:
        all_information = p.map(get_all_information, new_list)

    tittle = []
    creator = []
    stars = []
    company = []
    official_site = []
    more_like = []

    # all link information is added to the lists in the correct order
    for link_informations in all_information:
        count = 0
        for information in link_informations:
            if count == 0:
                tittle.append(information)
            elif count == 1:
                creator.append(information)
            elif count == 2:
                stars.append(information)
            elif count == 3:
                company.append(information)
            elif count == 4:
                more_like.append(information)
            else:
                official_site.append(information)
            count += 1

    # data frame is created using all collected information
    movie_list = pd.DataFrame(
        {"Tittle": tittle, "Creator": creator, "Stars": stars, "Production Companies": company, "Official Sites": official_site,
         "More Like This": more_like})
    # Saved to bucket as csv
    movie_list.to_csv(r"C:\Users\userpc\Desktop\Movies.csv")
    s3.upload_file(r'C:\Users\userpc\Desktop\Movies.csv', 'read-and-upload',
                   'IMDB_list/' + 'new_list_4' + '.csv')






