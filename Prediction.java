import java.util.HashMap;
import java.util.Map;

/**
 * Create prediction
 */
public class Prediction {


    private HashMap<Integer, HashMap<Integer, Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    private HashMap<Integer, HashMap<Integer, Double>> similarities;


    public Prediction(HashMap<Integer, HashMap<Integer, Double>> train,
                      HashMap<Integer, Double> avg, HashMap<Integer, HashMap<Integer, Double>> sim){
        trainingSet = train;
        userAverages = avg;
        similarities = sim;
    }

    public Prediction(HashMap<Integer, HashMap<Integer, Double>> train,
                      HashMap<Integer, HashMap<Integer, Double>> sim){
        trainingSet = train;
        similarities = sim;
    }


    public void setSimilarity(HashMap<Integer, HashMap<Integer, Double>> similarity){
        similarities = similarity;
    }

    private boolean checkItemExists(int user, int item){
        boolean exists = false;
        if(trainingSet.get(user).get(item)!=null){
            exists=true;
        }
        return  exists;
    }

    //calculates top half of sum
    private double topSum(int user_id, int item_id){
        double sum = 0.0;
        double bottom = 0.0;
        int user;
        double similarity;
        double rating;
        double user_average;

        HashMap<Integer, Double> user_sim = similarities.get(user_id);
        for(Map.Entry<Integer, Double> entry: user_sim.entrySet()){
            user = entry.getKey();
            similarity = entry.getValue();
            if(checkItemExists(user, item_id)) {
                rating = trainingSet.get(user).get(item_id);
                user_average = userAverages.get(user);
                sum += similarity * (rating - user_average);
                bottom += similarity;
            }
        }
        if(bottom!=0.0) {
            sum = sum / bottom;
        }
        else{
            sum = 0.0;
        }
        return sum;
    }


    private double itemSum(int user_id, int item_id){
        double sum;
        double top = 0.0;
        double bottom = 0.0;
        double rating;
        double similarity;
        int item;

        HashMap<Integer, Double> item_sim = similarities.get(item_id);
        for(Map.Entry<Integer, Double> entry: item_sim.entrySet()){
            item = entry.getKey();
            similarity = entry.getValue();
            if(checkItemExists(user_id, item)) {
                rating = trainingSet.get(user_id).get(item);
                top += similarity * rating;
                bottom += similarity;
            }
        }

        if(bottom!=0.0) {
            sum = top / bottom;
        }
        else{
            sum = 0.0;
        }

        return sum;
    }

    //calculates prediction using user_id and item_id
    public double total(int user_id, int item_id, String type){
        double total;
        double sum;

        //Check if running user or item based system
        if(type.equals("user")) {
            sum = topSum(user_id, item_id);
            //Set prediction as average rating
            total = userAverages.get(user_id) + sum;
        }else{
            total = itemSum(user_id,item_id);
        }
        if(total>10){
            total=10;
        }
        if(total<0){
            total=0;
        }
        return total;
    }
}
