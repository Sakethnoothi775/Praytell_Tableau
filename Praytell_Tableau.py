
import functions_framework
from google.cloud import storage
import pandas as pd
import io
import os
from google.cloud.sql.connector import Connector, IPTypes
import pymysql
import sqlalchemy

@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    client = storage.Client()
    BUCKET = client.get_bucket('drive_csv_files')
    blob = BUCKET.blob(name)
    content = blob.download_as_string()

    dataframe = pd.read_csv(io.StringIO(content.decode('utf-8')))

    # Sanitize column names
    dataframe.columns = dataframe.columns.str.replace("'", "", regex=False)  # Remove single quotes
    dataframe.columns = dataframe.columns.str.replace(" ", "_", regex=False)  # Replace spaces with underscores

    instance_connection_name = 'helpful-passage-431912-r1:us-central1:praytell-tableau'
   

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # Creating SQLAlchemy engine
    connector = Connector()
    def getconn():
        conn = connector.connect(
            instance_connection_name,
            "pymysql",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type
        )
        return conn

    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn
    )

    # Write dataframe to the SQL table
    dataframe.to_sql(name=name.replace('.csv', ''), con=pool, index=False, if_exists='append')
    connector.close()
    print("Sucessfully stored the file into the databases please check the SQL database")
