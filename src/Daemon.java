import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Daemon {

    static final int MASTER = 2;
    static final int WORKER = 1;
    static String ID;
    static Integer myHashValue;
    static int joinPortNumber;
    static int packetPortNumber;
    static int filePortNumber;
    static int masterPortNumber;
    static int graphPortNumber;
    static final List<String> neighbors = new ArrayList<>();
    // membership list is a map beteween node ID to <counter, timestamp>
    static final Map<String, long[]> membershipList = new HashMap<>();
    static final Map<String, Integer> masterList = new HashMap<>();
    static final Map<String, String> workerList = new HashMap<>();
    // hashValues is a map between a string's hashvalue to the string
    static final TreeMap<Integer, String> hashValues = new TreeMap<>();
    static PrintWriter fileOutput;
    static String[] hostNames;
    static String master = "None";
    final static int bufferSize = 512;
    static boolean neighborUpdated = false;

    public Daemon(String configPath) {

        // check if the configPath is valid
        if (!(new File(configPath).isFile())) {
            System.err.println("No such file!");
            System.exit(1);
        }

        Properties config = new Properties();

        try (InputStream configInput = new FileInputStream(configPath)) {

            // load the configuration
            config.load(configInput);
            hostNames = config.getProperty("hostNames").split(":");
            joinPortNumber = Integer.parseInt(config.getProperty("joinPortNumber"));
            packetPortNumber = Integer.parseInt(config.getProperty("packetPortNumber"));
            filePortNumber = Integer.parseInt(config.getProperty("filePortNumber"));
            masterPortNumber = Integer.parseInt(config.getProperty("masterPortNumber"));
            graphPortNumber = Integer.parseInt(config.getProperty("graphPortNumber"));
            String logPath = config.getProperty("logPath");

            // output the configuration setting for double confirmation
            System.out.println("Configuration file loaded!");
            System.out.println("Introducers are:");
            for (int i = 0; i < hostNames.length; i++) {
                String vmIndex = String.format("%02d", (i + 1));
                System.out.println("VM" + vmIndex + ": " + hostNames[i]);
            }
            System.out.println("Join Port Number: " + joinPortNumber);
            System.out.println("Packet Port Number: " + packetPortNumber);

            // assign daemon process an ID: the IP address
            ID = LocalDateTime.now().toString() + "#" +
                    InetAddress.getLocalHost().getHostName();
            myHashValue = Hash.hashing(ID, 8);
            // assign appropriate log file path
            File outputDir = new File(logPath);
            if (!outputDir.exists())
                outputDir.mkdirs();
            fileOutput = new PrintWriter(new BufferedWriter(new FileWriter(logPath + "result.log")));
            File sdfsDir = new File("../SDFS");
            if (!sdfsDir.exists()) {
                boolean result = false;
                try {
                    result = sdfsDir.mkdir();
                } catch (SecurityException se) {
                    System.out.println("Can't create the SDFS directory.");
                }
                if (result) System.out.println("Directory created");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void displayPrompt() {
        System.out.println("===============================");
        System.out.println("Please input the commands:.....");
        System.out.println("Enter \"join\" to join to group......");
        System.out.println("Enter \"member\" to show the membership list");
        System.out.println("Enter \"id\" to show self's ID");
        System.out.println("Enter \"leave\" to leave the group");
        System.out.println("Enter \"put localfilename sdfsfilename\" to put a file in this SDFS");
        System.out.println("Enter \"get sdfsfilename localfilename\" to fetch a sdfsfile to local system");
        System.out.println("Enter \"delete sdfsfilename\" to delete the sdfsfile");
        System.out.println("Enter \"ls sdfsfilename\" to show all the nodes which store the file");
        System.out.println("Enter \"store\" to list all the sdfsfiles stored locally");
        System.out.println("Enter \"sava task(pagerank/sssp) taskparam localgraphfile outputsdfsfilename");
        System.out.println("===============================");
    }

    public static void main(String[] args) {

        boolean isIntroducer = false;
        boolean isMaster = false;
        String configPath = null;

        // parse the input arguments
        if (args.length == 0 || args.length > 3) {
            System.err.println("Please enter the argument as the following format: <configFilePath> <-i> <-m>");
            System.exit(1);
        }

        configPath = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);
        StringBuilder builder = new StringBuilder();
        for (String arg: args) builder.append(arg);
        String setting = builder.toString();
        if (setting.indexOf("-i") != -1) {
            isIntroducer = true;
            System.out.println("Set this node as an introducer!");
        }
        if (setting.indexOf("-m") != -1) {
            isMaster = true;
            System.out.println("Set this node as a master!");
        }
        if (!setting.equals("") && !isIntroducer && !isMaster) {
            System.err.println("Could not recognize the input argument");
            System.err.println("Please enter the argument as the following format: <configFilePath> <-i> <-m>");
            System.exit(1);
        }

        Daemon daemon = new Daemon(configPath);
        displayPrompt();

        try (BufferedReader StdIn = new BufferedReader(new InputStreamReader(System.in))) {

            // prompt input handling
            String cmd;
            while ((cmd = StdIn.readLine()) != null) {

                String[] cmdParts = cmd.trim().split(" +");

                switch (cmdParts[0]) {
                    case "join":
                        // to deal with the case that users enter "JOIN" command multiple times
                        if (membershipList.size() == 0) {
                            ExecutorService mPool =
                                    Executors.newFixedThreadPool(
                                            5 + ((isIntroducer)? 1: 0) + ((isMaster)? 2: 0));
                            mPool.execute(new FileServer());
                            if (isMaster) mPool.execute(new Master());
                            DaemonHelper.joinGroup(isIntroducer, isMaster);
                            if (isIntroducer) mPool.execute(new IntroducerThread());
                            if (isMaster) mPool.execute(new MasterSyncThread());
                            mPool.execute(new GraphServer());
                            mPool.execute(new ListeningThread());
                            mPool.execute(new HeartbeatThread(100));
                            mPool.execute(new MonitorThread());
                            System.out.println("join successfully");
                        } else System.out.println("Duplicated join!");

                        break;

                    case "member":
                        System.out.println("===============================");
                        System.out.println("Membership List:");
                        int size = hashValues.size();
                        Integer[] keySet = new Integer[size];
                        hashValues.navigableKeySet().toArray(keySet);
                        for (Integer key : keySet) {
                            System.out.println(hashValues.get(key) +
                                    Arrays.toString(membershipList.get(hashValues.get(key))) + "/" + key);
                        }
                        System.out.println("===============================");
                        System.out.println("Neighbor List:");
                        for (String neighbor: neighbors) {
                            System.out.println(neighbor);
                        }
                        System.out.println("===============================");
                        System.out.println("Master:");
                        System.out.println(master);
                        System.out.println("===============================");
                        break;

                    case "id":
                        System.out.println("ID: " + ID);
                        break;

                    case "leave":
                        if (membershipList.size() != 0) {
                            Protocol.sendGossip(ID, "Leave", membershipList.get(ID)[0] + 10,
                                    membershipList.get(ID)[1],3, 4, new DatagramSocket());
                        }
                        fileOutput.println(LocalDateTime.now().toString() + " \"LEAVE!!\" " + ID);
                        fileOutput.close();
                        System.exit(0);

                    case "put":
                        DaemonHelper.writeLog(cmd, "");
                        userCommand.putFile(cmdParts);
                        break;
                    case "get":
                        DaemonHelper.writeLog(cmd, "");
                        userCommand.getFile(cmdParts);
                        break;
                    case "delete":
                        DaemonHelper.writeLog(cmd, "");
                        userCommand.deleteFile(cmdParts);
                        break;
                    case "ls":
                        DaemonHelper.writeLog(cmd, "");
                        userCommand.listFile(cmdParts);
                        break;
                    case "store":
                        DaemonHelper.writeLog(cmd, "");
                        System.out.println("SDFS files stored at this node are: ");
                        for (String s : FilesOP.listFiles("../SDFS/"))
                            System.out.println(s);
                        System.out.println("===============================");
                        break;
                    case "sava":
                        userCommand.savaGraph(cmdParts);
                        break;
                    default:
                        System.out.println("Unsupported command!");
                        displayPrompt();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
