import sys
import csv

from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

csv.field_size_limit(sys.maxsize)

argLength = len(sys.argv)
if argLength <= 1:
    print("Usage: ")
    print(sys.argv[0] + " OPCODE [HOSTNAME] [BUCKET_NAME] [USERNAME PASSWORD]")
    print("Defaults: ")
    print(sys.argv[0] + " OPCODE localhost proj admin adminadmin")
    print("")
    print("Opcodes:")
    print(sys.argv[0] + " 1             Query 1 (Compute ticket number, issue date and time for all citations where the fine amount is over 350:)")
    print(sys.argv[0] + " 2             Query 2 (Compute make and average fine amount for each car make in the database)")
    print(sys.argv[0] + " 3             Query 3 (Compute the number of citations per day and violation code (alternatively: compute day and violation)")
    print(sys.argv[0] + " 4             Query 4 (Compute the number of citations per day and violation code (alternatively: compute day and violation)")
    print(sys.argv[0] + " 5             Query 5 (Count tickets with fine amount greater equal 100 dollars and cars not registered in California which are charged for parking on park places for people with disabilities (handicapped persons))")
    print(sys.argv[0] + " 6             Upload data (data.csv in same dir) and create primary index")
    print(sys.argv[0] + " 7             Create indices")

else:
    opCode = sys.argv[1]

    host = 'localhost'
    bucketName = 'proj'

    username = 'admin'
    password = 'adminadmin'
    if argLength > 2:
        host = sys.argv[2]

    if argLength > 3:
        bucketName = sys.argv[3]

    if argLength > 5:
        username = sys.argv[4]
        password = sys.argv[5]

    cluster = Cluster('couchbase://' + host)
    authenticator = PasswordAuthenticator(username, password)
    cluster.authenticate(authenticator)

    bucket = cluster.open_bucket(bucketName)
    bucket.timeout = 900
    bucket.n1ql_timeout = 900


    if opCode == "1":
        rows = bucket.n1ql_query('SELECT ticket_number FROM `' + bucketName + '` WHERE fine_amount > 350')
        for row in rows:
            print(row)
    elif opCode == "2":
        rows = bucket.n1ql_query('SELECT make Make, AVG(fine_amount) AvgFine FROM `' + bucketName + '` GROUP BY make ORDER BY AvgFine ASC')
        for row in rows:
            print(row)
    elif opCode == "3":
        rows = bucket.n1ql_query('SELECT issue_date, violation_code, count(ticket_number) FROM `' + bucketName + '` GROUP BY issue_date, violation_code;')
        for row in rows:
            print(row)
    elif opCode == "4":
        rows = bucket.n1ql_query('SELECT count(ticket_number)  FROM `' + bucketName + '`  WHERE fine_amount >= 100 and rp_state_plate != “CA”;')
        for row in rows:
            print(row)
    elif opCode == "5":
        rows = bucket.n1ql_query('SELECT count(ticket_number), avg(fine_amount)  FROM `' + bucketName + '` WHERE fine_amount >= 100 and rp_state_plate != “CA” and contains(violation_description, “DISABLED PARKING”);')
        for row in rows:
            print(row)
    elif opCode == "6":
        counter = 0
        print("Inserting data...")

        with open('data.csv', newline='') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in spamreader:

                if counter > 0:

                    fa = 0
                    if row[16].isdigit():
                        fa = int(row[16])
                    try:
                        bucket.upsert(row[0], {'ticket_number': row[0],
                                       'issue_date': row[1],
                                       'issue_time': row[2],
                                       'meter_id': row[3],
                                       'marked_time': row[4],
                                       'rp_state_plate': row[5],
                                       'plate_expiry_date': row[6],
                                       'vin': row[7],
                                       'make': row[8],
                                       'body_style': row[9],
                                       'color': row[10],
                                       'location': row[11],
                                       'route': row[12],
                                       'agency': row[13],
                                       'violation_code': row[14],
                                       'violation_description': row[15],
                                       'fine_amount': fa,
                                       'latitude': row[17],
                                       'longitude': row[18]
                                       })

                    except:
                        print('Error at line ' + str(counter))
                counter += 1

                if counter % 10000 == 0:
                    print('Line: ' + str(counter))

        print("Data insert done.")

        print("Create primary index. This might take some time")
        manager = bucket.bucket_manager()
        manager.n1ql_index_create_primary(ignore_exists=True)

    elif opCode == "7":

        print("Create index on fine_amount. This might take even longer")
        manager = bucket.bucket_manager()

        manager.n1ql_index_create('fa', defer=False, ignore_exists=True, fields=['fine_amount'], primary=False)



    else:
        print("Invalid OpCode")
    print("Done")


