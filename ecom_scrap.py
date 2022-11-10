import mysql.connector
import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.oauth2 import service_account
from mysql.connector import errorcode
import prefect
from prefect import flow, task,prefect

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}

@task
def scrap_listing():

    baseurl = "https://webscraper.io"
    print("Scrape the listings and load the data !")
    data = requests.get('https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops').text
    soup=BeautifulSoup(data,'html.parser')
    productlist = soup.find_all("div",{"class":"thumbnail"})

    client =  connect_bq()

    product_links = []
    product_ids = []
    product_names = []
    for product in productlist:
        link = product.find("a",{"class":"title"}).get('href')           
        names = product.find("a",{"class":"title"}).get('title')   
        url = baseurl + link     
        product_links.append(url)
        id = link.rsplit('/', 1)[-1]
        product_ids.append(id)
        product_names.append(names)
            
        if not check_record_listings(id, client):

            
            rows_to_insert=[
                {'product_ids': id,'product_names':names,'product_links':url}
            ]
            errors  = client.insert_rows_json('scrap_data.scrap_listing',rows_to_insert)
            if errors == []:
                pass
            else:
                print("Encountered errors while inserting rows: {}".format(errors))

 
    print("Scrape the listings and load the data  Successfully!")
 

@task
def scrap_data():
    client =  connect_bq()

    sql_select_Query = "select product_links from scrap_data.scrap_listing "
    query_job = client.query(sql_select_Query)
    result = query_job.result()
    
    
    for link in result:
        data = requests.get(link.product_links,headers=headers).text
        soup=BeautifulSoup(data,'html.parser')

        id = link.product_links.rsplit('/', 1)[-1]
        try:
            price=soup.find("h4",{"class":"price"}).text 
        except:
            price = None

        try:
            name=soup.find_all("h4")[1].text 
        except:
            name = None

        try:
            description=soup.find("p",{"class":"description"}).text
        except:
            description = None
        
        try:
            rating=soup.find("div",{"class":"ratings"}).text.strip()
        except:
            rating=None


        if not check_record_data(id, client):            
            rows_to_insert=[
                {'id': id,'name': name,'price':price,'rating':rating,'description':description}
            ]
            errors  = client.insert_rows_json('scrap_data.scrap_data',rows_to_insert)
            if errors == []:
                pass
            else:
                print("Encountered errors while inserting rows: {}".format(errors))

    print("Scrape the data and load the data  Successfully!")



def check_record_listings(product_ids, client):
    # Checking if the Record doesn't already exists
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("product_ids", "INT64", product_ids),
    ])
    query_job = client.query("SELECT product_ids FROM scrap_data.scrap_listing WHERE product_ids = @product_ids",job_config=job_config)
    results = query_job.result()
    return (results.total_rows>0)

def check_record_data(id, client):
    # Checking if the Record doesn't already exists
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("id", "INT64", id),
    ])
    query_job = client.query("SELECT id FROM scrap_data.scrap_data WHERE id = @id",job_config=job_config)
    results = query_job.result()
    return (results.total_rows>0)

@task
def analysis():
    client =  connect_bq()

    sql_select_Query = """Select sl.product_ids,sd.name,sd.price,sd.rating,sd.description
                        From scrap_data.scrap_listing sl
                        Inner Join scrap_data.scrap_data sd
                        On sl.product_names = sd.name limit 10"""
    query_job = client.query(sql_select_Query)
    result = query_job.result()
    for row in result:
        print( row.product_ids,'   ', row.name ,'   ',row.price ,'   ', row.rating ,'   ', row.description )
    print("Run an SQL command which will create a join table where the product names match showing only 10 recods!")


def connect_bq():
    
    credentials = service_account.Credentials.from_service_account_file('credible-skill-368207-c724c020981c.json')

    project_id = 'credible-skill-368207'
    client = bigquery.Client(credentials= credentials,project=project_id)

    return client


@flow
def main_flow():
    # scrap_listing()
    # scrap_data()
    analysis() 


main_flow()