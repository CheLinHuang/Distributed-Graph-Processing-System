# Sava - Distributed Graph Processing System
The Distributed Graph Processing System that process large graphs, for arbitrary functions.

## Package Dependencies
- Java 8

## Instructions
### Step 1 - SetUp
1. Type ```git clone git@gitlab.engr.illinois.edu:fa17-cs425-g04/MP4.git``` to download the files to local machine.
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

### Step 4 - Run Master Daemon Process
1. Type ```cd $ROOT/src/```
2. Type ```java Daemon ../config/config.properties -i``` to run the master daemon process. The master daemon process has extra functionality in addition to regular daemon process, which is allowing client to send Sava request and organize workers to fulfill the task.
3. Upon the prompt shows, enter "ID" to show the machine ID and enter "JOIN" to run the daemon process and join the group.
4. If there is no introducer alive, you would not able to join the group.

### Step 5 - Run Daemon Process
1. Type ```cd $ROOT/src/```
2. Type ```java Daemon ../config/config.properties``` to run the daemon process.
3. Upon the prompt shows, enter "ID" to show the machine ID and enter "JOIN" to run the daemon process and join the group.
4. If there is no introducer alive, you would not able to join the group.

### Step 6 - Enter Command
Enter "join" to join to group
Enter "member" to show the membership list
Enter "id" to show self's ID
Enter "leave" to leave the group
Enter "put localfilename sdfsfilename" to put a file in this SDFS
Enter "get sdfsfilename localfilename" to fetch a sdfsfile to local system
Enter "delete sdfsfilename" to delete the sdfsfile
Enter "ls sdfsfilename" to show all the nodes which store the file
Enter "store" to list all the sdfsfiles stored locally
Enter "sava task(pagerank/sssp) taskparam localgraphfile outputsdfsfilename" to run Sava task with given parameters