import java.util.HashMap;
import java.util.Map;

/**
 * Create prediction here
 */
public class Prediction {


    private HashMap<Integer, HashMap<Integer, Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    private HashMap<Integer, HashMap<Integer, Double>> similarities;


    public Prediction(HashMap<Integer, HashMap<Integer, Double>> berlin,
                      HashMap<Integer, Double> dortmund, HashMap<Integer, HashMap<Integer, Double>> munich){
        trainingSet = berlin;
        userAverages = dortmund;
        similarities = munich;
    }

    public Prediction(HashMap<Integer, HashMap<Integer, Double>> berlin,
                      HashMap<Integer, HashMap<Integer, Double>> munich){
        trainingSet = berlin;
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
                //we didn't set the user average here, whoops
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
        double sum = 0.0;
        double top = 0.0;
        double bottom = 0.0;
        double rating = 0.0;
        double similarity = 0.0;
        int item = 0;

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
        double total = 0.0;
        double sum;

        if(type.equals("user")) {
            sum = topSum(user_id, item_id);
            total = userAverages.get(user_id) + sum; //Set prediction as average rating
        }else{
            total = itemSum(user_id,item_id);
        }
        if(total>10){
            total=10;
        }
        if(total<0){
            total=0;
        }

        //System.out.println("Sum = " + sum);
        return total;
    }
    //5.93576564765119 is average rating in trainingSet
}
