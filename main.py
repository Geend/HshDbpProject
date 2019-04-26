import sys

from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

argLength = len(sys.argv)
if argLength <= 1:
    print("Please provide Opcode")
    print(sys.argv[0] + " 1             Query 1 (Compute ticket number, issue date and time for all citations where the fine amount is over 350:)")
    print(sys.argv[0] + " 2             Query 2 (Compute make and average fine amount for each car make in the database)")
    print(sys.argv[0] + " 3             Query 3 (Compute the number of citations per day and violation code (alternatively: compute day and violation)")
    print(sys.argv[0] + " 4             Upload data (data.csv in same dir)")
else:
    opCode = sys.argv[1]
    cluster = Cluster('couchbase://localhost')
    authenticator = PasswordAuthenticator('admin', 'adminadmin')
    cluster.authenticate(authenticator)
    bucket = cluster.open_bucket('proj')

    if opCode == "1":
        rows = bucket.n1ql_query('SELECT ticket_number FROM `proj` WHERE fine_amount > 350')
        for row in rows:
            print(row)
    elif opCode == "2":
        rows = bucket.n1ql_query('SELECT make Make, AVG(fine_amount) AvgFine FROM `proj` GROUP BY make ORDER BY AvgFine ASC')
        for row in rows:
            print(row)
    elif opCode == "3":
        rows = bucket.n1ql_query('SELECT issue_date, violation_code, count(ticket_number) FROM `proj` GROUP BY issue_date, violation_code;')
        for row in rows:
            print(row)
    elif opCode == "4":
        print("Done")
    else:
        print("Invalid OpCode")
    print("Done")


