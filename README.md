# Simple Distributed File System
The Simple Distributed File System(SDFS) is a distributed file system that data stored in SDFS is tolerant up to two machine failures at a time. It intended to be scalable as the number of servers increases. After failure(s), it ensure that data is re-replicated quickly so that another (set of) failures that happens soon after is tolerated. It provides fast time-bounded write-write conflicts.

## Package Dependencies
- Java 8

## Instructions
### Step 1 - SetUp
1. Type ```git clone git@gitlab.engr.illinois.edu:fa17-cs425-g04/MP3.git``` to download the files to local machine.
2. Type ```cd $ROOT/src``` where ```$ROOT``` is the project root directory.
3. Type ```javac Daemon.java``` to compile files.

### Step 2 - Edit Configuration File
There are 5 lines in the file: ```hostNames```, ```joinPortNumber```, ```packetPortNumber```, ```filePortNumber```, and ```logPath```. ```hostNames``` defines the introducer machines in the distributed group membership system, ```joinPortNumber``` defines which port the introducer listens to new member join request, ```packetPortNumber``` defines which port the daemon process listens to heartbeat and gossip, ```filePortNumber``` defines which port the daemon process listens to file related request, and ```logPath``` defines the path to the system log in each machine.

1. Type ```cd $ROOT/config/```
2. Type ```vim config.properties``` to edit the configuration file.

### Step 3 - Run Introducer Daemon Process
1. Type ```cd $ROOT/src/```
2. Type ```java Daemon ../config/config.properties -i``` to run the introducer daemon process. The introducer daemon process has extra functionality in addition to regular daemon process, which is allowing new machine to join the group.
3. Upon the prompt shows, enter "ID" to show the machine ID and enter "JOIN" to run the introducer daemon process and join the group.

### Step 4 - Run Daemon Process
1. Type ```cd $ROOT/src/```
2. Type ```java Daemon ../config/config.properties``` to run the daemon process.
3. Upon the prompt shows, enter "ID" to show the machine ID and enter "JOIN" to run the daemon process and join the group.
4. If there is no introducer alive, you would not able to join the group.

### Step 5 - Enter Command
Enter "join" to join to group
Enter "member" to show the membership list
Enter "id" to show self's ID
Enter "leave" to leave the group
Enter "put localfilename sdfsfilename" to put a file in this SDFS
Enter "get sdfsfilename localfilename" to fetch a sdfsfile to local system
Enter "delete sdfsfilename" to delete the sdfsfile
Enter "ls sdfsfilename" to show all the nodes which store the file
Enter "store" to list all the sdfsfiles stored locally
