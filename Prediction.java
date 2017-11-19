import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Create prediction here
 */
public class Prediction {


    private HashMap<Integer, HashMap<Integer, Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    private HashMap<Integer, HashMap<Integer, Double>> similarities;


    public Prediction(HashMap berlin, HashMap dortmund, HashMap munich){
        trainingSet = berlin;
        userAverages = dortmund;
        similarities = munich;
    }


    public void setSimilarity(HashMap<Integer, HashMap<Integer, Double>> similarity){
        similarities = similarity;
    }

    private boolean checkItemExists(int user, int item){
        return trainingSet.get(user).get(item) !=null;
    }

    //calculates top half of sum
    private double topSum(int user_id, int item_id){
        double sum = 0.0;
        int user = 0;
        double similarity = 0.0;
        double rating = 0.0;
        double user_average = 0.0;

        HashMap<Integer, Double> user_sim = similarities.get(user_id);
        for(Map.Entry<Integer, Double> entry: user_sim.entrySet()){
            user = entry.getKey();
            similarity = entry.getValue();
            if(checkItemExists(user, item_id)) {
                rating = trainingSet.get(user).get(item_id);
                //we didn't set the user average here, whoops
                user_average = userAverages.get(user);
                sum += similarity * (rating - user_average);
            }
        }

        return sum;
    }

    //calculates bottom half of sum
    private double bottomSum(int user_id, int item_id){
        double sum = 0.0;

        HashMap<Integer, Double> user_sim = similarities.get(user_id);

        for(Map.Entry<Integer, Double> entry: user_sim.entrySet()){
            int user = entry.getKey();
            //Added user have same item check
            if(checkItemExists(user, item_id)) {
                sum += entry.getValue();
            }
        }

        return sum;
    }

    //calculates prediction using user_id and item_id
    public double total(int user_id, int item_id){
        double sum = 0.0;
        double topSum = 0.0;
        double bottomSum = 0.0;

        topSum = topSum(user_id, item_id);
        bottomSum = bottomSum(user_id, item_id);

        sum = (userAverages.get(user_id) + (topSum/bottomSum));
        return sum;
    }

}
