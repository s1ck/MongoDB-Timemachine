Testbed 3 - Multiple Server

Server 1

CPU: 4x Intel(R) Xeon(R) CPU E5649  @ 2.53GHz
RAM: 6GB
HDD: 80GB (noSSD)

Server 2

CPU: 2x Intel(R) Xeon(R) CPU E31220 @ 3.10GHz
RAM: 2GB
HDD: 10GB (noSSD)

Server 3

CPU: 2x Intel(R) Xeon(R) CPU E31220 @ 3.10GHz
RAM: 2GB
HDD: 10GB (noSSD)

Instances

mongos instance 				@Server1
mongod configsvr 				@Server1
mongod instance (--nojournal) 	@Server2
mongod instance (--nojournal) 	@Server3
