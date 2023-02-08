#please do not save this file as impala.py
# Install Libraries
!pip3 install impyla==0.17.0 --proxy http://pxuser:squidpassword@squid.central-tools.infra.cloudera.com:3128
!pip3 install thrift==0.11.0 --proxy http://pxuser:squidpassword@squid.central-tools.infra.cloudera.com:3128
!pip3 install thrift_sasl==0.4.3 --proxy http://pxuser:squidpassword@squid.central-tools.infra.cloudera.com:3128
!pip3 install --upgrade kerberos>=1.3.0 --proxy http://pxuser:squidpassword@squid.central-tools.infra.cloudera.com:3128
!pip install pandas

import os
import pandas
import re
from impala.dbapi import connect
from impala.util import as_pandas
import time

# Connect to Impala
conn = connect(host=os.getenv("IMPALA_DAEMON"),
        port=21050,
        auth_mechanism='GSSAPI',
        use_ssl=True,
        kerberos_service_name='impala')
 
# Execute using SQL
cursor = conn.cursor()

cursor.execute('show databases')
db = as_pandas(cursor)
print(db)

cursor.execute('show tables')
tables = as_pandas(cursor)
print(tables)

cursor.execute('CREATE TABLE IF NOT EXISTS Persons (PersonID int,Name STRING,City STRING);')
create_table = as_pandas(cursor)
print(create_table)

cursor.execute('describe Persons')
desc_table = as_pandas(cursor)
print(desc_table)

time.sleep(20)
#cursor.execute('DROP TABLE Persons')
#drop_table = as_pandas(cursor)
#print(drop_table)

cursor.execute('SELECT * FROM Persons')
table_content = as_pandas(cursor)
print(table_content)

insert_data = 'INSERT INTO Persons (personid, name, city) VALUES (%s, %s, %s)'
cursor.execute(insert_data, (1, 'A', 'abc'))

insert_data = 'INSERT INTO Persons (personid, name, city) VALUES (%s, %s, %s)'
cursor.execute(insert_data, (1, 'B', 'def'))

cursor.execute('SELECT * FROM Persons')
table_content = as_pandas(cursor)
print(table_content)
