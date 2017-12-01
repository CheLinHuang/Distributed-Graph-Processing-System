import java.io.*;
import java.net.Socket;
import java.util.*;

public class MasterSyncThread extends Thread {

    @Override
    public void run() {
        System.out.println("MasterSyncThread established");
        while (true) {
            try{
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // do nothing
            }
            if (!Daemon.master.equals(Daemon.ID)) {
                Map<String, long[]> tempFileList = null;
                Map<String, List<String>> tempFileReplica = null;
                List<String> tempTaskInfo = null;
                Integer tempIteration = null;
                String tempWorkers = null;
                Map<Integer, Vertex> tempGraph = null;
                Map<Integer, String> tempPartition = null;
                boolean update = true;
                try {

                    Socket masterSkt = new Socket(
                            Daemon.master.split("#")[1], Daemon.masterPortNumber);
                    DataOutputStream masterOut =new DataOutputStream(masterSkt.getOutputStream());
                    masterOut.writeUTF("SYNC_PULL");
                    ObjectInputStream masterIn = new ObjectInputStream(masterSkt.getInputStream());

                    tempFileList = (HashMap<String, long[]>) masterIn.readObject();
                    tempFileReplica = (HashMap<String, List<String>>) masterIn.readObject();
                    tempTaskInfo = (List<String>) masterIn.readObject();
                    tempIteration = masterIn.readInt();
                    tempWorkers = masterIn.readUTF();
                    tempGraph = (HashMap<Integer, Vertex>) masterIn.readObject();
                    tempPartition = (HashMap<Integer, String>) masterIn.readObject();
                    System.out.println("=========================");
                    System.out.println("Sync Results:");
                    System.out.println(tempFileList.toString());
                    System.out.println(tempFileReplica.toString());
                    System.out.println(tempWorkers);

                } catch (Exception e) {
                    // if during the synchronization, the original master fails
                    // abort the synchronization
                    update = false;
                }
                if (update) {
                    Master.fileList = tempFileList;
                    Master.fileReplica = tempFileReplica;
                    Master.taskInfo = tempTaskInfo;
                    Master.iteration = tempIteration;
                    Master.workers = tempWorkers;
                    Master.graph = tempGraph;
                    Master.partition = tempPartition;
                }
            }
        }
    }

}
