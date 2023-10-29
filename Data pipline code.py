#!/usr/bin/env python
# coding: utf-8

# In[2]:


import boto3
import pandas as pd
from io import StringIO


# In[3]:


AWS_REGION = "us-east-1"
SCHEMA_NAME = "covid_database"
S3_STAGING_DIR = "s3://output-bucket-for-athena-covid19-dataset/output"
S3_BUCKET_NAME = "output-bucket-for-athena-covid19-dataset"
S3_OUTPUT_DIRECTORY = "output"


# In[4]:


athena_client = boto3.client(
    "athena",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)


# In[5]:


Dict = {}
def download_and_load_query_results(
    client: boto3.client, query_response: Dict
) -> pd.DataFrame:
    while True:
        try:
            client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
    temp_file_location: str = "athena_query_results.csv"
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    s3_client.download_file(
        S3_BUCKET_NAME,
        f"{S3_OUTPUT_DIRECTORY}/{query_response['QueryExecutionId']}.csv",
        temp_file_location,
    )
    return pd.read_csv(temp_file_location)    


# In[6]:


response1 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."us_states";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[12]:


response2 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."enigma_jhud" limit 10000;',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[14]:


response3 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."states_daily";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[22]:


response4 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."state_abv";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[24]:


response5 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."us_daily";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[28]:


response6 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."us_county";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[30]:


response7 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."us_total_latest";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[32]:


response8 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."rearc_usa_hospital_beds";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[34]:


response9 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."countypopulation";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[36]:


response10 = athena_client.start_query_execution(
    QueryString='SELECT * FROM "covid-database"."countrycode";',
    QueryExecutionContext={"Database": SCHEMA_NAME},
    ResultConfiguration={
        "OutputLocation": S3_STAGING_DIR,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
    },
)


# In[37]:


countrycode = download_and_load_query_results(athena_client, response10)


# In[35]:


countypopulation = download_and_load_query_results(athena_client, response9)


# In[9]:


us_states = download_and_load_query_results(athena_client, response1)


# In[25]:


us_daily = download_and_load_query_results(athena_client, response5)


# In[29]:


us_country = download_and_load_query_results(athena_client, response6)


# In[31]:


us_total_latest = download_and_load_query_results(athena_client, response7)


# In[13]:


enigma_jhud = download_and_load_query_results(athena_client, response2)


# In[15]:


state_daily = download_and_load_query_results(athena_client, response3)


# In[23]:


state_abv = download_and_load_query_results(athena_client, response4)


# In[46]:


state_abv.head(1)


# In[47]:


new_header = state_abv.iloc[0]


# In[48]:


new_header


# In[19]:


#Scaling dataframe


# In[49]:


state_abv = state_abv[1:]
state_abv.columns = new_header


# In[50]:


state_abv.head()


# In[53]:


state_abv.head(1)


# In[52]:


dimDate = state_daily[['fips','date']]


# In[54]:


dimDate.head(1)


# In[66]:


dimDate['date'] = pd.to_datetime(dimDate['date'], format='%Y%m%d')


# In[68]:


type(dimDate['date'])


# In[69]:


dimDate['year'] = dimDate['date'].dt.year
dimDate['month'] = dimDate['date'].dt.month
dimDate['day_of_week'] = dimDate['date'].dt.dayofweek


# In[70]:


dimDate


# In[57]:


factCovid_1 = enigma_jhud[['fips','province_state','country_region','confirmed','deaths','recovered','active']]
factCovid_2 = state_daily[['fips','date','positive','negative','hospitalizedcurrently','hospitalized','hospitalizeddischarged']]
factCovid = pd.merge(factCovid_1, factCovid_2, on='fips', how='inner')


# In[59]:


factCovid.shape


# In[62]:


dimRegion_1 = enigma_jhud[['fips','province_state','country_region','latitude','longitude']]
dimRegion_2 = us_country[['fips','county','state']]
dimRegion = pd.merge(dimRegion_1, dimRegion_2, on='fips', how='inner')


# In[64]:


dimRegion.shape


# In[72]:


#Our data warehouse
#dimDate
#dimRegion
#factCovid


# In[73]:


bucket = "corona19-bucket"


# In[75]:


#Store factCovid fact table in S3
csv_buffer = StringIO()
factCovid.to_csv(csv_buffer)
s3_resource = boto3.resource('s3',
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION,
                            )
s3_resource.Object(bucket, 'output/factCovid.csv').put(Body=csv_buffer.getvalue())


# In[81]:


#Store dimDate dimension table in S3
csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3_resource = boto3.resource('s3',
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION,)
s3_resource.Object(bucket, 'output/dimDate.csv').put(Body=csv_buffer.getvalue())


# In[83]:


#Store dimRegion dimension table in S3
csv_buffer = StringIO()
dimDate.to_csv(csv_buffer)
s3_resource = boto3.resource('s3',
                             aws_access_key_id=AWS_ACCESS_KEY,
                             aws_secret_access_key=AWS_SECRET_KEY,
                             region_name=AWS_REGION,)
s3_resource.Object(bucket, 'output/dimRegion.csv').put(Body=csv_buffer.getvalue())


# In[84]:


dimDatesql = pd.io.sql.get_schema(dimDate.reset_index(), 'dimDate')
print(''.join(dimDatesql))


# In[89]:


dimRegionsql = pd.io.sql.get_schema(dimRegion.reset_index(), 'dimRegion')
print(''.join(dimRegionsql))


# In[91]:


factCovidsql = pd.io.sql.get_schema(factCovid.reset_index(), 'factCovid')
print(''.join(factCovidsql))


# In[97]:


import redshift_connector
import psycopg2


# In[99]:


try:
    conn = psycopg2.connect(host="my-first-workgroup.us-east-1.redshift-serverless.amazonaws.com", dbname="dev", port=5439)
except psycopg2.Error as e:
    print("Error")
    print(e)
    
conn.set_session(autocommit=True)


# In[101]:


cursor = redshift_connector.Cursor = conn.cursor()


# In[102]:


cursor


# In[103]:


cursor.execute("""
CREATE TABLE "dimDate" (
"index" INTEGER,
  "fips" REAL,
  "date" TIMESTAMP,
  "year" INTEGER,
  "month" INTEGER,
  "day_of_week" INTEGER
)
""")


# In[104]:


cursor.execute("""
CREATE TABLE "dimRegion" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "latitude" REAL,
  "longitude" REAL,
  "county" TEXT,
  "state" TEXT
)
""")


# In[105]:


cursor.execute("""
CREATE TABLE "factCovid" (
"index" INTEGER,
  "fips" REAL,
  "province_state" TEXT,
  "country_region" TEXT,
  "confirmed" REAL,
  "deaths" REAL,
  "recovered" REAL,
  "active" REAL,
  "date" INTEGER,
  "positive" INTEGER,
  "negative" REAL,
  "hospitalizedcurrently" REAL,
  "hospitalized" REAL,
  "hospitalizeddischarged" REAL
)
""")


# In[114]:


try:
    cursor.execute("""
    copy dimDate from 's3://corona19-bucket/output/dimDate.csv'
    credentials 'aws_iam_role=arn:aws:iam:: :role/redshift-s3-Access'
    delimiter ','
    region 'us-east-1'
    IGNOREHEADER 1
     """)
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[119]:


try:
    cursor.execute("""
    copy dimRegion from 's3://corona19-bucket/output/dimRegion.csv'
    credentials 'aws_iam_role=arn:aws:iam:: :role/redshift-s3-Access'
    delimiter ','
    region 'us-east-1'
    IGNOREHEADER 1
     """) 
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[122]:


try:
    cursor.execute("""
    copy dimRegion from 's3://corona19-bucket/output/dimRegion.csv'
    credentials 'aws_iam_role=arn:aws:iam:: :role/redshift-s3-Access'
    delimiter ','
    region 'us-east-1'
    IGNOREHEADER 1
     """)
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[125]:


try:
    cursor.execute("""
    copy dimRegion from 's3://corona19-bucket/output/dimRegion.csv'
    credentials 'aws_iam_role=arn:aws:iam:: :role/redshift-s3-Access'
    delimiter ','
    region 'us-east-1'
    IGNOREHEADER 1
     """)
except psycopg2.Error as e:
    print("Error: issue creating table")
    print(e)


# In[ ]:




