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
    
