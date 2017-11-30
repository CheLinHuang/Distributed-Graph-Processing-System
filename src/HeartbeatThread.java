import java.net.DatagramSocket;

public class HeartbeatThread extends Thread {

    // period is the period to send the heartbeat signal (in msec)
    private int period;
    private long counter;

    public HeartbeatThread(int period) {
        super("HeartbeatThread");
        this.period = period;
        this.counter = 1;
    }

    @Override
    public void run() {
        DatagramSocket sendSocket = null;

        try {
            sendSocket = new DatagramSocket();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        while (true) {
            try {

                // send the heartbeat every time period
                Thread.sleep(period);
                synchronized (Daemon.membershipList) {
                    long[] status = Daemon.membershipList.get(Daemon.ID);
                    Protocol.sendHeartBeat(Daemon.ID, counter++, status[1], sendSocket);
                    // update the counter and the timestamp

                    Daemon.membershipList.put(Daemon.ID,
                            new long[]{counter, status[1], System.currentTimeMillis()});
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}