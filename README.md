# snowflake-ingest
<h1>Data ingestion into Snowflake</h1>

<h2>A Quick Walkthrough</h2>

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

For Windows users, use the following command to generate your key pair:

	ssh-keygen -t rsa
 	type <path\to\dir>\.ssh\id_rsa.pub


<h3>Ingest via Python Script</h3>
