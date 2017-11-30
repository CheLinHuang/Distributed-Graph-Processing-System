import java.lang.Math;
import java.util.*;

public class Hash {

    public static int hashing(String inputString, int numOfBits) {

        long hashValue = 0;
        long modulus = (long) Math.pow(2, numOfBits);

        for (int i = 0; i < inputString.length(); i++) {
            hashValue = hashValue * 31 + inputString.charAt(i);
        }

        hashValue = hashValue % modulus;
        // in case that hashValue is negative
        if (hashValue < 0) hashValue += modulus;

        return (int)hashValue;
    }
    public static List<String> getTargetNode(int fileHashValue) {
        System.out.println(fileHashValue);
        synchronized (Daemon.hashValues) {
            int size = Daemon.hashValues.navigableKeySet().size();
            Integer[] keySet = new Integer[size];
            Daemon.hashValues.navigableKeySet().toArray(keySet);
            System.out.println(Arrays.toString(keySet));

            int min = keySet[0].intValue();
            int max = keySet[size - 1].intValue();
            List<String> targetNodes = new ArrayList<>();

            int targetIndex = -1;
            if (fileHashValue > max || fileHashValue < min) targetIndex = 0;
            else {
                for (int i = 1; i < size; i++) {
                    if (keySet[i].intValue() >= fileHashValue && keySet[i - 1].intValue() < fileHashValue) {
                        targetIndex = i;
                        break;
                    }
                }
            }
            targetNodes.add(Daemon.hashValues.get(keySet[targetIndex]));
            // the successors of the targetNode into the list
            for (int i = 0; i < 2; i++) {
                targetIndex = (targetIndex + 1) % size;
                // if the successor is equal to itself, this means that the number
                // of its successors is less than 2
                if (targetNodes.contains(Daemon.hashValues.get(keySet[targetIndex])))
                    break;
                targetNodes.add(Daemon.hashValues.get(keySet[targetIndex]));

            }
            return targetNodes;
        }
    }
}
