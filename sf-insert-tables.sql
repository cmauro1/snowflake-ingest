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