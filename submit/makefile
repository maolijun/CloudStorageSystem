all:
	g++ MasterServer.cpp -o MasterServer -lpthread
	g++ StorageServer.cpp BigTable.cpp Logger.cpp -o StorageServer -lpthread -lboost_system -lboost_filesystem -lboost_thread
	g++ WebServer.cc -o WebServer -lpthread
	g++ LoadBalancer.cc -o LoadBalancer -lpthread
	g++ smtp_project.cc -I/opt/local/include/ -L/opt/local/bin/openssl -o SmtpClient -lcrypto -lpthread -lresolv
	g++ admin.cc -o Admin -lpthread
clean:
	rm MasterServer
	rm StorageServer
	rm WebServer
	rm LoadBalancer
	rm SmtpClient
	rm Admin
