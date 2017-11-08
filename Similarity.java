import java.util.HashMap;

/**
 * Check similarity here
 */
public class Similarity {
    private HashMap<Integer, HashMap<Integer,Double>> ratingHash;

    public Similarity(HashMap amsterdam){
        ratingHash=amsterdam;
    }

    public void topHalfSum(int user1, int user2){
        HashMap<Integer,Double> user1Hash = ratingHash.get(user1);
        HashMap<Integer,Double> user2Hash = ratingHash.get(user2);


    }


}
