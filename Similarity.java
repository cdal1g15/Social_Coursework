import java.util.HashMap;
import java.util.Map;

/**
 * Check similarity here
 */
public class Similarity {
    private HashMap<Integer, HashMap<Integer,Double>> ratingHash;
    private HashMap<Integer, Double> userAverage;


    public Similarity(HashMap amsterdam, HashMap colorado){
        ratingHash=amsterdam;
        userAverage=colorado;
    }

    public Double sumTotal(int user1, int user2){
        HashMap<Integer,Double> user1Hash = ratingHash.get(user1);
        HashMap<Integer,Double> user2Hash = ratingHash.get(user2);
        HashMap<Integer,Double> user1SimItemsHash = intersectMaps(user1Hash, user2Hash);
        HashMap<Integer,Double> user2SimItemsHash = intersectMaps(user2Hash, user1Hash);
        Double user1Average = userAverage.get(user1);
        Double user2Average = userAverage.get(user2);
        //System.out.println(user1Average + " : " + user2Average);
        Double top = topHalfSum(user1SimItemsHash, user2SimItemsHash, user1Average, user2Average);
        if(top<0){
            return 0.0;
        }
        Double bottom = getBottomSum(user1SimItemsHash, user2SimItemsHash, user1Average, user2Average);

        Double answer = top/bottom;
        return answer;
    }


    public double topHalfSum(HashMap<Integer,Double> user1SimItemsHash, HashMap<Integer,Double> user2SimItemsHash,
                             Double user1Average, Double user2Average){
        Double sum = 0.0;
        /*
        printHashMap(user1SimItemsHash);
        System.out.println("user 2");
        printHashMap(user2SimItemsHash);
        */
        //calculate top half of equation
        for(Map.Entry<Integer, Double> entry1 : user1SimItemsHash.entrySet()){
            Integer item = entry1.getKey();
            Double user1Value = entry1.getValue();
            Double user2Value = user2SimItemsHash.get(item);
            //System.out.println("user1 :" + user1Value + " user2 :" + user2Value);
            sum+= (user1Value - user1Average) * (user2Value - user2Average);
        }
        return sum;
    }


    public double getBottomSum(HashMap<Integer,Double> user1SimItemsHash, HashMap<Integer,Double> user2SimItemsHash,
                               Double user1Average, Double user2Average){

        Double leftBottomSum=0.0;
        Double rightBottomSum=0.0;
        for(Map.Entry<Integer, Double> entry1 : user1SimItemsHash.entrySet()){
            Integer item = entry1.getKey();
            Double user1Value = entry1.getValue();
            Double user2Value = user2SimItemsHash.get(item);
            leftBottomSum+= Math.pow((user1Value - user1Average),2);
            rightBottomSum+= Math.pow((user2Value - user2Average),2);
        }
        Double sum = Math.sqrt(leftBottomSum)*Math.sqrt(rightBottomSum);
        return sum;
    }


    public void printHashMap(HashMap map){
        for (Object name : map.keySet()){
            String key = name.toString();
            String value = map.get(name).toString();
            System.out.println(key + " : " + value);
        }
    }


    public HashMap intersectMaps(HashMap a, HashMap b){
        HashMap results = new HashMap(a);
        results.keySet().retainAll(b.keySet());
        return results;
    }
}
