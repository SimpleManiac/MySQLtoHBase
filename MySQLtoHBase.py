import sys
import os
import MySQLdb
import re
import getopt
import subprocess
import json

def main(argv):

	hostaddress = "210.125.146.181"
	username = "root"
	password = "gac81-344"
	dbName = "POP"
	
	try:
      		opts, args = getopt.getopt(argv,"h:u:p:d:",["hostaddress=","username=","password", "database"])
   	except getopt.GetoptError:
		print 'This is basically free software. Feel free to use it for any purposes. Cheers Leo!'
		print 'Hadoop, HBase and SQOOP required.'
      		print 'MySQLtoHBase.py -h <hostname or ip address> -u <username> -p <password> -d <database name>'
      		sys.exit(2)
   	for opt, arg in opts:
      		if opt in ("-h", "--hostaddress"):
         		hostaddress = arg
      		elif opt in ("-u", "--username"):
        		username = arg
		elif opt in ("-p", "--password"):
			password = arg
		elif opt in ("-d", "--database"):
			dbName = arg	

	tableNames = []
	sqoopImport = []

	database = MySQLdb.connect(host=hostaddress,user=username,passwd=password)

	cursor = database.cursor()

	cursor.execute("USE " + str(dbName))

	cursor.execute("SHOW TABLES")

	
	for tablename in cursor:
		tbln = re.sub(r'[^\w]', '', str(tablename))

		sqoopcommand = "sqoop import --connect jdbc:mysql://" + hostaddress + "/" + dbName + " --username " + username + " --password " + password + " "
		sqoopcommand = sqoopcommand + "--table " + tbln + " --hbase-table " + tbln
	
		cursor.execute("SHOW columns FROM " + tbln)

		resultset = cursor.fetchall()
		hbasequery = "create "
		hbasequery = hbasequery + "'" + tbln  + "', "
		firstElement = True

		for column in resultset:
			if firstElement == True:
				sqoopcommand = sqoopcommand + " --column-family " + column[0] + " --driver com.mysql.jdbc.Driver"
				firstElement = False
			hbasequery = hbasequery + "'" + column[0]  + "', "

		firstElement = True
		sqoopImport.append(sqoopcommand)

		#print hbasequery[0:-2]
		#subprocess.call("echo " + json.dumps(hbasequery[0:-2]) + " | hbase shell", shell=True)
	
	for sqp in sqoopImport:
		print sqp
		os.system(sqp)
if __name__ == "__main__":
   main(sys.argv[1:])
