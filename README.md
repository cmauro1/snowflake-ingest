# snowflake-ingest
<h1>Data ingestion into Snowflake</h1>

<h2>A Quick Walkthrough</h2>

This demonstration will walk you through a simple data ingestion of raw json into Snowflake. All of the required files can be found in my repo, but the code and some shell commands will be provided in this walkthrough as well. Since the file is small, we will use a PUT command to add the file to a stage in Snowflake. If we were working with larger files, we could consider leveraging Snowpipe or Serverless Tasks to optimize on compute.

<h3>Dependencies</h3>
<ul>
  <li>Python 3.10 (other versions may cause dependency issues with underlying packages)</li>
  <li>Java JDK 11 or 17</li>
  <li>sample_video_data.jsonl</li>
</ul>

<h3>Environment Setup</h3>
<h4>First, start with creating your Python virtual environment.</h4>

  From your command line, navigate to your project directory.
  
  If Python 3.10 is in your PATH variable, execute the following command:
  
    python -m venv \sf-stage-venv
  If Python 3.10 is NOT in your PATH variable, you will have to invoke the full path to that version:
  
    <path\to\Python\3.10\python.exe> -m venv \sf-stage-venv

    
  Now activate your virtual environment with this command:
  
    sf-stage-venv\Scripts\activate


<h4>Next, pip install the required Python libraries.</h4> 

Thanfully, we can pip install from requirements.txt.

  If Python 3.10 is in your PATH variable:
  
    python -m pip install requirements.txt
    
  If Python 3.10 is NOT in your PATH variable:
  
    <path\to\Python\3.10\python.exe> -m pip install requirements.txt

    

<h3>Create Database Objects</h3>
<h4>Now we can create the database objects in Snowflake.</h4>

The tables will follow a star schema.
A fact table called FactWatchSession will store rows with a granularity of watch sessions, and will be described by dimension tables that capture video, channel, device, region, and date data.

Copy the following SQL and run it in a new Snowflake project:
```sql
  USE ACCOUNTADMIN;
  
  -- Begin setting up warehouse, database, schema, and role
  CREATE WAREHOUSE IF NOT EXISTS INGEST;
  CREATE ROLE IF NOT EXISTS INGEST;
  GRANT USAGE ON WAREHOUSE INGEST TO ROLE INGEST;
  GRANT OPERATE ON WAREHOUSE INGEST TO ROLE INGEST;
  CREATE DATABASE IF NOT EXISTS VIDEO;
  USE DATABASE VIDEO;
  CREATE SCHEMA IF NOT EXISTS VIDEO;
  USE SCHEMA VIDEO;
  GRANT OWNERSHIP ON DATABASE VIDEO TO ROLE INGEST;
  GRANT OWNERSHIP ON SCHEMA VIDEO.VIDEO TO ROLE INGEST;
  GRANT CREATE TABLE ON SCHEMA VIDEO.VIDEO TO ROLE INGEST;
  CREATE USER INGEST PASSWORD='<REDACTED>' LOGIN_NAME='INGEST' MUST_CHANGE_PASSWORD=FALSE, DISABLED=FALSE, DEFAULT_WAREHOUSE='INGEST', DEFAULT_NAMESPACE='INGEST.INGEST', DEFAULT_ROLE='INGEST';
  GRANT ROLE INGEST TO USER INGEST;
  SET USERNAME = (SELECT CURRENT_USER());
  GRANT ROLE INGEST TO USER IDENTIFIER($USERNAME);
  
  -- Create staging area to load files
  CREATE FILE FORMAT IF NOT EXISTS JSON
  TYPE = 'JSON';
  
  CREATE STAGE IF NOT EXISTS STG_VIDEO
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  FILE_FORMAT = JSON
  COMMENT = 'Stage for semi-structured data file';
  
  CREATE OR REPLACE TABLE RAW_VIDEO_EVENTS (
      raw VARIANT
  );
  
  -- Create all necessary tables
  CREATE TABLE IF NOT EXISTS DimVideo (
  video_id INT IDENTITY(1,1) PRIMARY KEY,
  source_video_id VARCHAR(255),
  title VARCHAR(255),
  category VARCHAR(255),
  publish_date DATETIME NULL,
  content_type VARCHAR(50)
  );
  
  CREATE TABLE IF NOT EXISTS DimChannel (
  channel_id INT IDENTITY(1,1) PRIMARY KEY,
  source_channel_id VARCHAR(255),
  channel_name VARCHAR(255),
  created_date DATETIME NULL,
  subscriber_count INT
  );
  
  CREATE TABLE IF NOT EXISTS DimDevice (
  device_id INT IDENTITY(1,1) PRIMARY KEY,
  device_type VARCHAR(25),
  OS VARCHAR(15) NULL
  );
  
  CREATE TABLE IF NOT EXISTS DimRegion (
  region_id INT IDENTITY(1,1) PRIMARY KEY,
  country_code VARCHAR(2),
  country VARCHAR(50),
  region VARCHAR(50) NULL,
  continent VARCHAR(15)
  );
  
  CREATE TABLE IF NOT EXISTS DimDate (
  date_id INT IDENTITY(1,1) PRIMARY KEY,
  date DATE,
  day_of_week VARCHAR(10),
  week int,
  month VARCHAR(10),
  quarter TINYINT,
  year INT,
  is_weekend BOOLEAN
  );
  
  CREATE TABLE IF NOT EXISTS FactWatchSession (
  duration_minutes INT,
  views INT,
  likes INT,
  comments INT,
  video_id INT,
  channel_id INT,
  device_id INT,
  region_id INT,
  date_id INT,
  CONSTRAINT fk_video
      FOREIGN KEY (video_id)
      REFERENCES DimVideo(video_id),
  CONSTRAINT fk_channel
      FOREIGN KEY (channel_id)
      REFERENCES DimChannel(channel_id),
  CONSTRAINT fk_device
      FOREIGN KEY (device_id)
      REFERENCES DimDevice(device_id),
  CONSTRAINT fk_region
      FOREIGN KEY (region_id)
      REFERENCES DimRegion(region_id),
  CONSTRAINT fk_date
      FOREIGN KEY (date_id)
      REFERENCES DimDate(date_id)
  );
```

<h4>Generate a key pair for the Snowflake connector API.</h4>

I used an RSA key pair to establish a connection to my Snowflake account. There might be other ways to capture credentials, feel free to explore those options if you so please. I will provide steps for generating and capturing the key pair for both Windows and Linux users.

For <b>Windows</b> users:

	::Generate the key pair, these will be the credentials to your account
	ssh-keygen -t rsa
 
 	::This command prints your public key to the console, copy it to your clipboard
 	type <path\to\dir>\.ssh\id_rsa.pub

Once you have copied your public key to your clipboard, paste it into this DDL over <public key> and execute in Snowflake:
```sql
ALTER USER INGEST SET RSA_PUBLIC_KEY=CONCAT('-----BEGIN PUBLIC KEY-----', '<public key>', '-----END PUBLIC KEY-----');
```

For <b>Linux</b> users:

	#Generate the key pair, these will be the credentials to your account
 	openssl genrsa 4096 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
	openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

    #Prints your DDL right to the console, copy and paste into Snowflake and execute
	echo "ALTER USER INGEST SET RSA_PUBLIC_KEY='`cat ./rsa_key.pub`';"

<h3>Ingest via Python Script</h3>

<h4>Prepare a .env file</h4>

We will create a .env in the project folder for our Python script to capture the our private key.

Paste the following into your .env, replacing <account> with your snowflake account Id and <private key> with your generated private key:

	SNOWFLAKE_ACCOUNT=<account>
	SNOWFLAKE_USER=INGEST
	PRIVATE_KEY=<private key>

<h4>Create the Python script</h4>

Start a Python script called py_addtostage.py in your project folder. We will execute the script from the command line and pass the name of the data file as an argument to that execution call.

```python
import os, sys, logging
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
from cryptography.hazmat.primitives import serialization

logging.basicConfig(level=logging.WARN)


def connect_snow():
    
    private_key = "-----BEGIN PRIVATE KEY-----\n" + os.getenv("PRIVATE_KEY") + "\n-----END PRIVATE KEY-----\n)"
    p_key = serialization.load_pem_private_key(
        bytes(private_key, 'utf-8'),
        password=None
    )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role=os.getenv('ROLE'),
        database='VIDEO',
        schema='VIDEO',
        warehouse=os.getenv('WAREHOUSE'),
    )


if __name__ == "__main__":    
    args = sys.argv[1:]
    file_path = args[0]
    stage_name = 'STG_VIDEO'
    snow = connect_snow()
    cursor = snow.cursor()

    # Load file to stage in Snowflake using PUT command
    try:
        put_command = f'PUT file://{file_path} @{stage_name} auto_compress=true;'
        cursor.execute(put_command)
        print(f'File {file_path} successfully uploaded to stage {stage_name}.')
    except Exception as e:
        print(f'Error uploading file: {e}')
    finally:
        snow.close()
```

<h4>Ingest the data file</h4>

While in your project directory from the command line, execute the following command:

	py_addtostage.py sample_video_data.jsonl

After a few minutes, you should get a nice little message that says the file was successfully uploaded to our stage in Snowflake.
To verify, you can run this command in Snowflake:

```sql
LIST @STG_VIDEO;
```

<h3>Populate Snowflake Tables</h3>

<h4>Move raw json into a table</h4>

Data in its raw form is not super useful here. It would be, in most cases, far more useful if we could query the data from a table. Use the following SQL to move the raw json data into a table that we can actually query from:

```sql
COPY INTO RAW_VIDEO_EVENTS
FROM @STG_VIDEO
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
ON_ERROR = 'CONTINUE';  -- Optional: skip bad records
```

<h4>Populate the dimension tables</h4>

Now that the data is maleable, lets populate the dimension (dim) tables. Populating the dim tables will assign Ids to unique records, enforced by the Identity constraint on all the Id fields (not to be confused with the source Id fields, which were the original Ids taken from the data extract). Execute the following list of SQL to populate the dim tables:

```sql
INSERT INTO DimChannel (Source_Channel_Id, Channel_Name, Created_Date, Subscriber_Count)
SELECT DISTINCT raw:channel.channel_id::STRING AS source_channel_id,
				raw:channel.channel_name::STRING AS channel_name,
				GETDATE() AS Created_Date, -- In a production environment, pull this from the source or only insert new records here if daily batch
				NULL AS Subscriber_Count   -- Consider dropping this column if we don't have data
FROM RAW_VIDEO_EVENTS
ORDER BY 1,2;

-- Date is driven by the date of insert, which could be viable for a daily batch
INSERT INTO DimDate (Date, Day_Of_Week, Week, Month, Quarter, Year, Is_Weekend)
SELECT GETDATE() AS Date,
	   DAYNAME(GETDATE()) AS Day_Of_Week,
	   WEEKOFYEAR(GETDATE()) AS Week,
	   MONTH(GETDATE()) AS Month,
	   QUARTER(GETDATE()) AS Quarter,
	   YEAR(GETDATE()) AS Year,
	   CASE DAYNAME(GETDATE())
			WHEN 'Sat' THEN True
			WHEN 'Sun' THEN True
			ELSE False
	   END AS Is_Weekend;

INSERT INTO DimDevice (Device_Type, OS)
SELECT DISTINCT ws.value:device::STRING AS device,
				NULL AS OS -- Consider dropping this column if we don't have data
FROM RAW_VIDEO_EVENTS,
	 LATERAL FLATTEN(input => raw:watch_sessions) ws;

INSERT INTO DimRegion (Country_Code, Country, Region, Continent)
SELECT DISTINCT 
				ws.value:region::STRING AS country_code,
				CASE ws.value:region::STRING 
					WHEN 'AU' THEN 'Australia'
					WHEN 'BR' THEN 'Brazil'
					WHEN 'CA' THEN 'Canada'
					WHEN 'DE' THEN 'Denmark' 
					WHEN 'IN' THEN 'India'
					WHEN 'UK' THEN 'United Kingdom'
					WHEN 'US' THEN 'United States'
				END AS Country, 
				NULL AS region, -- Consider dropping this column if we don't have data                                
				CASE ws.value:region::STRING 
					WHEN 'AU' THEN 'Australia'
					WHEN 'BR' THEN 'South America'
					WHEN 'CA' THEN 'North America'
					WHEN 'DE' THEN 'Europe' 
					WHEN 'IN' THEN 'Asia'
					WHEN 'UK' THEN 'Europe'
					WHEN 'US' THEN 'North America'
				END AS continent
FROM RAW_VIDEO_EVENTS,
	 LATERAL FLATTEN(input => raw:watch_sessions) ws;

INSERT INTO DimVideo (Source_Video_Id, Title, Category, Publish_Date, Content_Type)
SELECT DISTINCT
				raw:video_id::STRING AS source_video_id,
				raw:title::STRING AS title,
				raw:category::STRING AS category,
				GETDATE() AS publish_date,
				CASE 
					WHEN LOWER(raw:title::STRING) LIKE '% guide%'       THEN 'Tutorial'
					WHEN LOWER(raw:title::STRING) LIKE '% walkthrough%' THEN 'Tutorial'
					WHEN LOWER(raw:title::STRING) LIKE '% fails%'       THEN 'Entertainment'
					WHEN LOWER(raw:title::STRING) LIKE '% top%moments%' THEN 'Entertainment'
					WHEN LOWER(raw:title::STRING) LIKE '% live%'        THEN 'Livestream'
				ELSE 'Informative'
	END AS content_type
FROM RAW_VIDEO_EVENTS;
```

<h4>Populate the fact table</h4>

Now that we have populated all of our dim tables and table Ids have been assigned, we will populate the fact table. Note the query optimization happening during this insert. Several joins are needed in order to capture the foreign keys for this fact table. The expressions needed to transform the data from RAW_VIDEO_EVENTS are very expensive and slow the query down to a halt. My solution to this is to perform those expressions ahead of time into a temp table, and populate the fact table from the temp table. This speeds the query up to finish in seconds, then we drop the temp table upon completion.

Use the following SQL to populate the fact table:

```sql
CREATE TEMPORARY TABLE tmp_ws (
	duration_minutes INT,
	views INT,
	likes INT,
	comments INT,
	source_video_id VARCHAR(10),
	source_channel_id VARCHAR(10),
	channel_name VARCHAR(255),
	device_type VARCHAR(25),
	country_code VARCHAR(2),
	date DATE
);
GRANT OWNERSHIP ON TABLE tmp_ws TO ROLE INGEST;

INSERT INTO tmp_ws (duration_minutes, views, likes, comments, Source_Video_Id, Source_Channel_Id, Channel_Name, Device_Type, Country_Code, Date)
SELECT 
	   ws.value:duration_minutes::INT AS duration_minutes,
	   raw:engagement.views::INT AS views,
	   raw:engagement.likes::INT AS likes,
	   raw:engagement.comments::INT AS comments,
	   raw:video_id::STRING AS source_video_id,
	   raw:channel.channel_id::STRING AS source_channel_id,
	   raw:channel.channel_name::STRING AS channel_name,
	   ws.value:device::STRING AS device_type,
	   ws.value:region::STRING AS country_code,
	   GETDATE() AS date
FROM RAW_VIDEO_EVENTS e,
	 LATERAL FLATTEN(input => raw:watch_sessions) ws;

INSERT INTO FactWatchSession (Duration_Minutes, Views, Likes, Comments, Video_Id, Channel_Id, Device_Id, Region_Id, Date_Id)
SELECT 
	   duration_minutes,
	   views,
	   likes,
	   comments,
	   video_id,
	   channel_id,
	   device_id,
	   region_id,
	   date_id
FROM tmp_ws t
JOIN DimVideo v USING (Source_Video_Id)
JOIN DimChannel c USING (Source_Channel_Id, Channel_Name)
JOIN DimDevice d USING (Device_Type)
JOIN DimRegion r USING (Country_Code)
JOIN DimDate dt USING (date);

DROP TABLE tmp_ws;
```

<h4>Deactivate your Python venv</h4>

Run the following command to deactivate the Python virtual environment:

	sf-stage-venv\Scripts\deactivate

<h3>Conculsion</h3>

So there you have it, a raw data file added to Snowflake, and data transformed and loaded into tables. If we wanted to automate this process further, we could wrap the SQL that populates the tables into a stored procedure and call it from our Python script. Lets say a new data file is dumped out to a directory every night, the Python script could then be called from a task-scheduling tool after that dump occurs, and voila, a nightly upload of new data. Of course we would want to either distinguish each new data set or exclude data that has already been loaded somehow. There are certainly ways of doing this depending on the needs of the business. 

Thank you for following along my Snowflake ingestion demonstration.
