import logging
import time
#import mysql.connector
import pymysql
from sshtunnel import SSHTunnelForwarder
#from paramiko import SSHClient
import paramiko


# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Log to console
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)

# Also log to a file
file_handler = logging.FileHandler("cpy-errors.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler) 


sql_hostname = 'dev.shilen.com'
sql_username = 'greg'
sql_password = 'Bl33dBlu3'
sql_main_database = 'weather'
sql_port = 3306
ssh_host = 'dev.shilen.com'
ssh_user = 'ubuntu'
ssh_port = 22
sql_ip = '1.1.1.1.1'

mypkey = paramiko.RSAKey.from_private_key_file('sandbox1.pem')



def connect_to_mysql(config, val, attempts=3, delay=2):
    
    
    try:
        with SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_user,
                ssh_pkey=mypkey,
                remote_bind_address=(sql_hostname, sql_port)) as tunnel:
            
            tunnel.start()
    
            print("tunnel");
        
            cnx = pymysql.connect(host='127.0.0.1', user=sql_username,
                    passwd=sql_password, db=sql_main_database,
                    port=tunnel.local_bind_port)
            
            with cnx:
                with cnx.cursor() as cursor:
                    
                    sql = "INSERT into sample ( sample_timestamp, station, temperature, humidity, dew_point, pressure, wind_speed, \
                                wind_direction, rain, wind_avg_mph, wind_max_mph ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
        
                    cursor.execute( sql, val )
                        
    
                cnx.commit()
                    
            tunnel.stop()
    except:
        print("unable to connect; we'll skip this record.")      
            
                
                
