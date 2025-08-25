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

<h4>Create table loading procedure.</h4>

A Python script will add a raw json file to stage, but the data needs to be loaded to tables so we can actually use it.

Copy the following SQL and run it to create a stored procedure to handle data loading:

```sql
CREATE OR REPLACE PROCEDURE SP_TRFM_STG_VIDEO () 
RETURNS STRING
AS 

BEGIN
	-- Move raw json data into a table where it can be queried
	COPY INTO RAW_VIDEO_EVENTS
	FROM @STG_VIDEO
	FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = FALSE)
	ON_ERROR = 'CONTINUE';  -- Optional: skip bad records

	-- Begin inserting rows into dimension tables
	INSERT INTO DimChannel (Source_Channel_Id, Channel_Name, Created_Date, Subscriber_Count)
	SELECT DISTINCT raw:channel.channel_id::STRING AS source_channel_id,
					raw:channel.channel_name::STRING AS channel_name,
					GETDATE() AS Created_Date, -- In a production environment, pull this from the source or only insert new records here if daily batch
					NULL AS Subscriber_Count   -- Consider dropping this column if we don't have data
	FROM RAW_VIDEO_EVENTS
	ORDER BY 1,2;

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
					NULL AS region,                               
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

	-- begin inserting rows into fact table
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
END;
```
<h3>Ingest via Python Script</h3>
