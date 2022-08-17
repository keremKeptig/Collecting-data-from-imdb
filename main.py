import boto3
from webCrawlerIMDB import get_all_information
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


def describe_ec2_instance():
    try:
        print("Describing EC2 instance")
        resource_ec2 = boto3.client("ec2")
        print(resource_ec2.describe_instances()["Reservations"][0]["Instances"][0]["InstanceId"])
        return str(resource_ec2.describe_instances()["Reservations"][0]["Instances"][0]["InstanceId"])
    except Exception as e:
        print(e)


def stop_ec2_instance():
    try:
        print("Stop EC2 instance")
        instance_id = describe_ec2_instance()
        resource_ec2 = boto3.client("ec2")
        print(resource_ec2.stop_instances(InstanceIds=[instance_id]))
    except Exception as e:
        print(e)


base_url = 'https://www.imdb.com/title/'
client = boto3.client('s3',
                        # Set up AWS credentials
                        aws_access_key_id="",
                         aws_secret_access_key="")

bucket_name = 'read-and-upload'

object_key = 'IMDB_list/new_diff_links.csv'
csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
body = csv_obj['Body']
csv_string = body.read().decode('utf-8')

# incomplete links
mergeString = csv_string.splitlines()

complete_url = []
for text in mergeString:
    complete_url.append(base_url + text + "/")

all_information = []
s3 = boto3.client('s3')

# create folder
s3.put_object(Bucket='read-and-upload', Key=('IMDB_list'+'/'))

if __name__ == '__main__':

    with ThreadPoolExecutor(max_workers=8) as p:
        # all information is kept in a list within a list
        all_information = p.map(get_all_information, complete_url)

    tittle = []
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
    movie_list.to_csv(r"C:\Users\userpc\Desktop\Movies.csv")
    s3.upload_file(r'C:\Users\userpc\Desktop\Movies.csv', 'read-and-upload',
                   'IMDB_list/' + 'all_info_new' + '.csv')

    # To shut down ec2 when execution is done
    stop_ec2_instance()

    

