import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;
import java.util.Arrays;

public class IntroducerThread extends Thread {

    @Override
    public void run() {

        DatagramSocket introducerSocket = null;

        // try until socket create correctly
        while (introducerSocket == null) {
            try {
                introducerSocket = new DatagramSocket(Daemon.joinPortNumber);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        byte[] receiveData = new byte[1024];
        byte[] sendData;

        // keep listening to the join request
        while (true) {

            try {

                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                introducerSocket.receive(receivePacket);

                // the new member's information
                // the format of joinID is: TimeStamp#Domain Name#isMaster
                String joinID = new String(receivePacket.getData(), 0, receivePacket.getLength());

                // extract new member's IP address
                InetAddress IPAddress = InetAddress.getByName(joinID.split("#")[1]);
                boolean isMaster = joinID.split("#")[2].equals("M") ? true : false;

                // assign status and add to the membership list
                long[] status = {0, isMaster? Daemon.MASTER: Daemon.WORKER, System.currentTimeMillis()};
                // System.out.println(Arrays.toString(status));
                Daemon.membershipList.put(joinID.substring(0, joinID.length()-2), status);
                Daemon.hashValues.put(Hash.hashing(joinID.substring(0, joinID.length()-2), 8),
                        joinID.substring(0, joinID.length()-2));
                if (isMaster) {
                    Daemon.masterList.put(joinID.substring(0, joinID.length() - 2),
                            Hash.hashing(joinID.substring(0, joinID.length()-2), 8));
                }
                // build the whole membership list string
                // the format of the string for each node is NodeID/counter#isMaster%
                StringBuilder sb = new StringBuilder();
                synchronized (Daemon.membershipList) {
                    for (Map.Entry<String, long[]> entry : Daemon.membershipList.entrySet()) {
                        sb.append(entry.getKey());
                        sb.append('/');
                        long[] memberStatus = entry.getValue();
                        sb.append(memberStatus[0]);
                        sb.append('/');
                        sb.append(memberStatus[1]);
                        sb.append('%');
                    }
                }

                // send the whole membership list back
                sendData = sb.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, receivePacket.getPort());
                introducerSocket.send(sendPacket);
                // send the current master back
                sendData = Daemon.master.getBytes();
                sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, receivePacket.getPort());
                introducerSocket.send(sendPacket);

                // Send gossip to old members to inform them a new node is coming
                Protocol.sendGossip(joinID.substring(0, joinID.length() - 2),
                        "Add", 0, isMaster? Daemon.MASTER: Daemon.WORKER,
                        3, 4, introducerSocket);

                // update my neighbor
                DaemonHelper.updateNeighbors();
                synchronized (Daemon.membershipList) {
                    if (!Daemon.membershipList.containsKey(Daemon.master))
                        DaemonHelper.masterElection();
                }
                DaemonHelper.checkReplica();
                DaemonHelper.writeLog("ADD", joinID);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
