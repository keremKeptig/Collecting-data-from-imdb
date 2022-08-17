import json
from bs4 import BeautifulSoup
import requests 
import boto3
import pandas as pd
import numpy
from concurrent.futures import ThreadPoolExecutor
import uuid
from time import sleep

base_url = "https://www.imdb.com/title/"


def get_all_information(url):

    all_information_string = []
    try:
        sleep(1)
        starting_url = requests.get(url)
        soup = BeautifulSoup(starting_url.content, "html.parser")

        # Creator information
        concatenate_creator = ""
        creators = soup.find("ul", {
            "class": "ipc-inline-list ipc-inline-list--show-dividers ipc-inline-list--inline "
                     "ipc-metadata-list-item__list-content baseAlt"})
        if creators is None:
            concatenate_creator += "There is no creator"
        else:
            exist_creators = creators.findAll(
                "a",
                {"class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

            for creator in exist_creators:
                if creator == exist_creators[len(exist_creators) - 1]:
                    concatenate_creator += creator.text
                else:
                    concatenate_creator += creator.text + "|"
        all_information_string.append(concatenate_creator)

        # Stars information
        star_class = soup.find("li", {"class": "ipc-metadata-list__item ipc-metadata-list-item--link"})

        concatenate_stars = ""
        if star_class is None:
            concatenate_stars += "there is no star"
        else:
            stars = star_class.findAll(("a", {
                "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"}))
            for star in stars:
                if star == stars[len(stars) - 1]:
                    concatenate_stars += star.text
                else:
                    concatenate_stars += star.text + "|"

        all_information_string.append(concatenate_stars)

        # Details information
        concatenate_company = ""
        details_section = soup.find("div", {"data-testid": "title-details-section"})  # details section
        if details_section is None:
            concatenate_company += "there is no production companies"
        else:
            details_companies = details_section.find("li", {"data-testid": "title-details-companies"})  # companies
            if details_companies is None:
                concatenate_company += "there is no production companies"
            else:
                production_companies = details_companies.findAll("a", {
                    "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

                for production_company in production_companies:
                    if production_company == production_companies[len(production_companies) - 1]:
                        concatenate_company += production_company.text
                    else:
                        concatenate_company += production_company.text + "|"

        all_information_string.append(concatenate_company)

        # Official Sites Details
        concatenate_sites = " "
        if details_section is None:
            concatenate_sites += "There is no official sites"
        else:
            details_class = details_section.find("li", {"data-testid": "details-officialsites"})
            if details_class is None:
                concatenate_sites += "There is no official sites"
            else:
                official_sites = details_class.findAll("a", {
                    "class": "ipc-metadata-list-item__list-content-item ipc-metadata-list-item__list-content-item--link"})

                for official_site in official_sites:
                    concatenate_sites += str(official_site.get('href')) + ','
        all_information_string.append(concatenate_sites)
        
    except Exception as e:
        pass
    
    return all_information_string
    
base_url = 'https://www.imdb.com/title/'
client = boto3.client('s3',
                        # Set up AWS credentials
                        aws_access_key_id="secret",
                         aws_secret_access_key="secret")

bucket_name = 'secret'

object_key = 'IMDB_list/new_diff_links.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

# incomplete links
mergeString = csv_string.splitlines()


s3 = boto3.resource('s3')
complete_url = []
for text in mergeString:
    complete_url.append(base_url + text + "/")

# create folder
s3.Bucket('read-and-upload').put_object(Key= 'IMDB_new'+'/')
new_list = []

def lambda_handler(event, context):
    
    
    start_point = int(event['Starting'])
    ending_point = int(event['EndingPoint'])
    name_of_file = event['ObjectName']
    name_of_current_file = event['Current']
    
   
    for link_information in range(start_point, ending_point):
        new_list.append(complete_url[link_information])
        
    transactionId = str(uuid.uuid1())
    #get data
    with ThreadPoolExecutor(max_workers=8) as p:
        # all information is kept in a list within a list
        all_information = p.map(get_all_information, new_list)
    
    creator = []
    stars = []
    company = []
    official_site = []

    # all link information is added to the lists in the correct order
    for link_information in all_information:
        creator.append(link_information[0])
        stars.append(link_information[1])
        company.append(link_information[2])
        official_site.append(link_information[3])

    
    # data frame is created using all collected information
    movie_list = pd.DataFrame(
        {"Creator": creator, "Stars": stars, "Production Companies": company, "Official Sites": official_site})
        
    # Saved to bucket as csv
    movie_list.to_csv('/tmp/' + name_of_current_file + '.csv',index=False)
    s3.Bucket('read-and-upload').upload_file('/tmp/' + name_of_current_file + '.csv', 'IMDB_new/' + name_of_file + '.csv')
    # response
    return {'Starting':start_point,'EndingPoint':ending_point, 'ObjectName':name_of_file, 'Success': 'true', 'TransactionId': transactionId}
    
    
    
    
