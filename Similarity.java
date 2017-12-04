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

    /*
    * x is user1/item1
    * y is user2/item2
    * */
    public Double sumTotal(int x, int y, String type){
        HashMap<Integer,Double> xHash = ratingHash.get(x);
        HashMap<Integer,Double> yHash = ratingHash.get(y);
        HashMap<Integer,Double> xSimHash = intersectMaps(xHash, yHash);
        HashMap<Integer,Double> ySimHash = intersectMaps(yHash, xHash);

        Double top =0.0;

        if(xSimHash.size()>0) {

            //uses different sum for
            if(type.equals("user")) {
                Double user1Average = userAverage.get(x);
                Double user2Average = userAverage.get(y);
                //System.out.println(user1Average + " : " + user2Average);
                top = topHalfSum(xSimHash, ySimHash, user1Average, user2Average);
            }else{
                top = itemSimTotal(xSimHash, ySimHash);
            }
        }
        return top;
    }

    private double itemSimTotal(HashMap<Integer,Double> item1SimUsersHash,
                                HashMap<Integer,Double> item2SimUsersHash){
        double answer = 0.0;
        double topSum = 0.0;
        double leftBottomSum=0.0;
        double rightBottomSum=0.0;

        for(Map.Entry<Integer,Double> entry1: item1SimUsersHash.entrySet()){
            Integer user = entry1.getKey();
            Double average = userAverage.get(user);
            Double item1rating = entry1.getValue();
            Double item2rating = item2SimUsersHash.get(user);

            topSum+= (item1rating - average) * (item2rating - average);
            leftBottomSum+= Math.pow((item1rating - average),2);
            rightBottomSum+= Math.pow((item2rating-average),2);
        }

        if(topSum>0) {
            Double bottomSum = Math.sqrt(leftBottomSum) * Math.sqrt(rightBottomSum);
            answer = topSum / bottomSum;
        }
        return answer;
    }

    private double topHalfSum(HashMap<Integer,Double> user1SimItemsHash, HashMap<Integer,Double> user2SimItemsHash,
                             Double user1Average, Double user2Average){
        Double sum = 0.0;
        Double leftBottomSum=0.0;
        Double rightBottomSum=0.0;
        for(Map.Entry<Integer, Double> entry1 : user1SimItemsHash.entrySet()){
            Integer item = entry1.getKey();
            Double user1Value = entry1.getValue();
            Double user2Value = user2SimItemsHash.get(item);
            sum+= (user1Value - user1Average) * (user2Value - user2Average);
            leftBottomSum+= Math.pow((user1Value - user1Average),2);
            rightBottomSum+= Math.pow((user2Value - user2Average),2);
        }
        Double answer = 0.0;
        if(sum>0) {
            Double bottom = Math.sqrt(leftBottomSum) * Math.sqrt(rightBottomSum);
            answer = sum / bottom;
        }
        return answer;
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
