import java.lang.Math;

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
    public static String getServer(int hashValue) {

        int size = Daemon.hashValues.navigableKeySet().size();
        Integer[] keySet = new Integer[size];
        Daemon.hashValues.navigableKeySet().toArray(keySet);

        int min = keySet[0].intValue();
        int max = keySet[size-1].intValue();

        if (hashValue > max) {
            return Daemon.hashValues.get(keySet[0]);
        }
        if (hashValue < min) {
            return Daemon.hashValues.get(keySet[size - 1]);
        }
        int targetIndex = -1;
        for (int i = 0; i < size; i++) {
            if (keySet[i].intValue() >= hashValue && keySet[i - 1].intValue() < hashValue) {
                targetIndex = i;
            }
        }
        return Daemon.hashValues.get(keySet[targetIndex]);
    }

}
