from time import sleep
import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
import csv


def more_like_this(url):
    sleep(1)
    all_information_string = []
    starting_url = requests.get(url)
    soup = BeautifulSoup(starting_url.content, "html.parser")

    all_information_string.append(url)
    all_information_string[0].replace('https://www.imdb.com/title/' , '')
    #more like this
    concatenate_tittle = " "
    if soup.findAll("div", {
        "class": "ipc-poster-card ipc-poster-card--base ipc-poster-card--dynamic-width ipc-sub-grid-item ipc-sub-grid-item--span-2"}) is None:
        concatenate_tittle += "There is no suggestions"

        all_information_string.append(" ")
    else:
        titleClass = soup.findAll("div", {
            "class": "ipc-poster-card ipc-poster-card--base ipc-poster-card--dynamic-width ipc-sub-grid-item ipc-sub-grid-item--span-2"})

        for i in range(len(titleClass)):
            title = titleClass[i].find("a", {
                "class": "ipc-poster-card__title ipc-poster-card__title--clamp-2 ipc-poster-card__title--clickable"})
            if i == (len(titleClass) - 1):
                concatenate_tittle += title.get('href')
            else:
                concatenate_tittle += str(title.get('href')) + ' | '
    all_information_string.append(concatenate_tittle)

    id = all_information_string[0]
    more_like = all_information_string[1]

    data = {
        '1': id,
        '2': more_like,
    }

    # Make data frame of above data
    df = pd.DataFrame(data, index=[0])

    # append data frame to CSV file
    df.to_csv(r"C:\Users\userpc\Desktop\Movies.csv", mode='a', index=True, header=False)

    return all_information_string


mergeString = []

with open(r'C:\Users\userpc\Desktop\new_diff_links.csv') as file:
    reader = csv.reader(file)
    #total links other csv
    for i in reader:
        mergeString.append('https://www.imdb.com/title/'+ i[0])

s3 = boto3.client('s3')

# create folder
s3.put_object(Bucket='read-and-upload', Key=('IMDB_list'+'/'))
if __name__ == '__main__':
    count = 0
    with ThreadPoolExecutor(max_workers=8) as p:
        # all information is kept in a list within a list
        all_information = p.map(more_like_this, mergeString)


    # data frame is created using all collected information
    s3.upload_file(r'C:\Users\userpc\Desktop\Movies.csv', 'read-and-upload',
                   'IMDB_list/' + 'appended_list' + '.csv')

