import mysql.connector
import logging as log

def connect_to_mysql(mysql_config):
    try:
        conn = mysql.connector.connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        log.info("Connected to MySQL")
        return conn
    except mysql.connector.Error as e:
        log.error(f"Error connecting to MySQL: {e}")
        raise

#                                                |>>>
#                                                |
#                                            _  _|_  _
#                                           |;|_|;|_|;|
#                                           \\.    .  /
#                                            \\:  .  /
#                                             ||:   |
#                                             ||:.  |
#                                             ||:  .|
#                                             ||:   |       \,/
#                                             ||: , |            /`\
#                                             ||:   |
#                                             ||: . |
#              __                            _||_   |
#     ____--`~    '--~~__            __ ----~    ~`---,              ___
#-~--~                   ~---__ ,--~'                  ~~----_____-~'   `~----~~
#Fortress of code, safeguarding our logic.
#Guarding the realm of variables and functions.
