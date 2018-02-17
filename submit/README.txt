PennCloud README


1, files:
makefile
config
http_config.txt
BigTable.h
BigTable.cpp
Logger.h
Logger.cpp
MasterServer.cpp
StorageServer.cpp
WebServer.cc
LoadBalancer.cc
smtp_project.cc
admin.cc


2, Compilation:
simply type make


3, External Libraies
We use boost library in backend. So you need to install boost library to run our codes. The install instruction can be found here: http://www.boost.org/doc/libs/1_61_0/more/getting_started/unix-variants.html
We do recommend to test our codes in Ubuntu or any other similar linux since you can use apt-get to install Boost easily.


4, Config Files
We have 2 config files: config and http_config.txt
a) config
The file “config” will be passed to master and admin console. In config, each line has two addresses separated by a comma. The two addresses in same line are in same replica group. And the last line is special, it only has one address. This special address is for smtp client.
An example:
127.0.0.1:7000, 127.0.0.1:7100
127.0.0.1:7001, 127.0.0.1:7101
127.0.0.1:5100
Here first two lines give two groups of servers. One has 7000 and 7100 as port. The other has 7001 and 7101 as port. The last line(5100) is smtp client.


b) http_config.txt
The file “http_config.txt” will be passed to each web server and the load balancer. In http_config.txt, each line has exactly one address. Load balancer will store each web server’s address, and each web server will find its own address with the -n command line argument, which indicates the line number in this file.
An exmaple:
127.0.0.1:10001
127.0.0.1:10002
127.0.0.1:10003
Here each line is the address of a web server. In the first line, 127.0.0.1 is ip address and 10001 is the port number for the first server.


5, How to Run our System
You need some terminals to run the entire system. One terminal for one command below(ORDER DOES MATTER):


./MasterServer -c ConfigFilePath -p Port [-v]
./LoadBalancer -c [ConfigFilePath] -p [port number] -a [admin console]
./WebServer -m [Master Port] -c [ConfigFilePath] -n [server index] -b [balancer port]
//////Run more WebServer here if you want to have more frontend servers)]
./admin -l [load balancer port] -p [admin Port] -m [master port]
./StorageServer -m masterAddress -a thisAddress [-v]
./StorageServer -m masterAddress -a thisAddress [-v] 
//////We run two Storage servers because they need to appear in pair for fault tolerance
//////Run more backend servers(in pair) here if you need, all allowed addresses are in config
./SmtpClient portNumber
//////Smtp Client only needs to be run if you want to send emails to gmail


Since each team member designs the command according to his own preference of style, the above command has some different styles for parameters. Now it seems we don’t have time to unify this before the deadline. WE SINCERELY APOLOGIZE FOR THIS AND PLEASE DO READ THE EXAMPLE BELOW TO RUN OUR CODES. WE HIGHLY RECOMMEND TO DIRECTLY USE THE COMMANDS BELOW WITH MINOR MODIFICATION(add storage servers or web servers).


EXAMPLE:
./MasterServer -p 5000 -c config -v
./LoadBalancer -c http_config.txt -p 9000 -a 8000
./WebServer -m 5000 -c http_config.txt -n 1 -b 9000
////More WebServer if you need, just give new -n)
./Admin -l 9000 -p 8000 -m 5000
./StorageServer -m 127.0.0.1:5000 -a 127.0.0:7000 -v
./StorageServer -m 127.0.0.1:5000 -a 127.0.0:7100 -v
////More StorageServer if you need, should appear in pairs and addresses should be correctly specified in file “config”
./SmtpClient 5100


So for this example, if you use default config and http_config.txt, master is on port 5000, load balancer is on 9000, admin is on 8000, webserver is on 10001, storage servers are on 7000 and 7100, smtp client is on 5100.