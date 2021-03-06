
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Database {


    private Connection c;
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private HashMap<Integer, HashMap<Integer, Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    private HashMap<Integer, HashMap<Integer, Double>> predictions;
    private ArrayList<Integer> testUsers;
    private ArrayList<Integer> testItems;
    private HashMap<Integer,ArrayList<Integer>> testSet;
    private final static String rec_system_type = "user";


    public static void main(String[] args) {
        Database db = new Database();
        db.loadTrainingSet("user");//takes ~25 seconds
                                        //different hashmap structure to user collab filtering
        db.loadUserAverages();
        db.loadUniqueTest(rec_system_type);
        //db.loadItemAverages();
        //db.loadUniqueTestUsers();
        db.loadTestSet();
        db.sizeOfSet(db.trainingSet);
        //db.storeSimilarity(rec_system_type);
        db.storePredictions(rec_system_type);
        db.addPredictedRatings();
    }


    //Initialise storage, connect to database
    private Database() {
        trainingSet = new HashMap<>();
        userAverages = new HashMap<>();
        predictions = new HashMap<>();
        testUsers = new ArrayList<>();
        testItems = new ArrayList<>();
        testSet = new HashMap<>();

        try {
            Class.forName(JDBC_DRIVER);
            String connection_string = "jdbc:sqlite:/home/conor/Documents/Social_Coursework/Social_Coursework/test.sl3";
            c = DriverManager.getConnection(connection_string);
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sizeOfSet(HashMap inputSet) {
        System.out.println(inputSet.size());
    }


    private void storeSimilarity(String field) {

        Similarity sim;
        ArrayList<Integer> uniqueSet;
        int noOfElements;

        //changes set based on which similarity we're getting
        if(field.equals("user")){
            uniqueSet = testUsers;
            noOfElements = userAverages.size();

        }else{
            uniqueSet = testItems;
            noOfElements = 80007;
        }

        sim = new Similarity(trainingSet, userAverages);

        String sql;
        PreparedStatement stmt;
        sql = "INSERT INTO simMatrix VALUES (?,?,?)";
        try {
            stmt = c.prepareStatement(sql);
            Long time = System.currentTimeMillis();
            int count =1;
            for (Integer value: uniqueSet) {
                int j=1;
                int limit =0;
                while( j < noOfElements && limit<2500) {
                    Double similarity = sim.sumTotal(value, j, field);
                    if(similarity > 0.3) {
                        stmt.setInt(1, value);
                        stmt.setInt(2, j);
                        stmt.setDouble(3, similarity);
                        stmt.addBatch();
                        limit++;
                    }
                    j++;
                }
                System.out.println(limit);
                Long end = System.currentTimeMillis();
                System.out.println((end - time) / 1000 + "seconds") ;
                System.out.println("Done user " + value + " number " + count);

                count++;
                stmt.executeBatch();
                c.commit();
            }
            Long end = System.currentTimeMillis();
            System.out.println((end-time)/1000 + "seconds");
        }catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * Loads into Java, userAverages table
     */
    private void loadUserAverages() {
        try {
            String sql = "SELECT * FROM user_averages";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while (rs.next()) {
                userAverages.put(rs.getInt("user_id"), rs.getDouble("avg"));
            }
            System.out.println("User Averages loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadItemAverages() {
        try {
            String sql = "SELECT DISTINCT item_id FROM testTable ORDER BY item_id ASC";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while (rs.next()) {
                testItems.add(rs.getInt("item_id"));
            }
            System.out.println(testItems.size());
            System.out.println("Items loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadTestSet() {
        try {
            int user = 0;
            String sql = "SELECT  user_id,item_id FROM testTable ORDER BY user_id";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while (rs.next()) {
                int nextUser = rs.getInt("user_id");
                if(user==nextUser){
                    testSet.get(nextUser).add(rs.getInt("item_id"));
                }
                else{
                    user =nextUser;
                    testSet.put(rs.getInt("user_id"), new ArrayList<>());
                    testSet.get(rs.getInt("user_id")).add(rs.getInt("item_id"));
                }

            }
            System.out.println("Test Set loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadUniqueTest(String field) {

        String columnName = field + "_id";
        try {
            String sql = "SELECT DISTINCT " +columnName+" FROM testTable";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while (rs.next()) {
                testUsers.add(rs.getInt(columnName));
            }
            System.out.println("Test "+field+" loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }


    //Loads trainingSet in java
    private void loadTrainingSet(String type) {
        int user = 0;
        int nextUser;

        String field1;
        String field2;

        if(type.equals("user")){
            field1 = "user_id";
            field2 = "item_id";
        }else{
            field1 = "item_id";
            field2 = "user_id";
        }

        try {
            String sql = "SELECT * FROM trainingSet ORDER BY " + field1 + " ASC ";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            //Checks if nextUser is CurrentUser, if so adds item to CurrentUsers hash, else moves on
            while (rs.next()) {
                nextUser = rs.getInt(field1);

                if (user == nextUser) {
                    trainingSet.get(user).put(rs.getInt(field2), rs.getDouble("rating"));
                } else {
                    user = nextUser;
                    trainingSet.put(user, new HashMap<>());
                    trainingSet.get(user).put(rs.getInt(field2), rs.getDouble("rating"));
                }
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Training Set loaded.");
        System.out.println(trainingSet.size());
    }


    //loads from database the similarity measures for a user
    public HashMap<Integer, HashMap<Integer, Double>> loadUserSimilarities(int test_user){
        int user = 0;
        int nextUser;
        HashMap<Integer, HashMap<Integer, Double>> similarities = new HashMap<>();
        try { //gets all similarities for a user
            String sql = "SELECT * FROM simMatrix WHERE user_id=?";

            PreparedStatement stmt = c.prepareStatement(sql);
            stmt.setInt(1,test_user);

            ResultSet rs = stmt.executeQuery();
            c.commit();

            //Checks if nextUser is CurrentUser, if so adds item to CurrentUsers hash, else moves on
            while (rs.next()) {
                nextUser = rs.getInt("user_id");

                if (user == nextUser) {
                    similarities.get(user).put(rs.getInt("user2_id"), rs.getDouble("prediction"));
                } else {
                    user = nextUser;
                    similarities.put(user, new HashMap<>());
                    similarities.get(user).put(rs.getInt("user2_id"), rs.getDouble("prediction"));
                }
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
        return similarities;
    }


    //loads from database the similarity measures for an item
    public HashMap<Integer, HashMap<Integer, Double>> loadItemSimilarities(int test_item){
        int item = 0;
        int nextItem;
        HashMap<Integer, HashMap<Integer, Double>> similarities = new HashMap<>();
        try { //gets all similarities for an item
            String sql = "SELECT * FROM simMatrixItems WHERE item1=?";

            PreparedStatement stmt = c.prepareStatement(sql);
            stmt.setInt(1,test_item);

            ResultSet rs = stmt.executeQuery();
            c.commit();

            //Checks if nextUser is CurrentUser, if so adds item to CurrentUsers hash, else moves on
            while (rs.next()) {
                nextItem = rs.getInt("item1");

                if (item == nextItem) {
                    similarities.get(item).put(rs.getInt("item2"), rs.getDouble("prediction"));
                } else {
                    item = nextItem;
                    similarities.put(item, new HashMap<>());
                    similarities.get(item).put(rs.getInt("item2"), rs.getDouble("prediction"));
                }
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
        return similarities;
    }

    //stores predictions calculated in predictions hash map
    private void storePredictions(String type){
        int userID = 0;
        int nextUser;
        ArrayList<Integer> itemID;
        double prediction;
        Prediction pred;

        if(type.equals("user")){
            pred = new Prediction(trainingSet,userAverages,null);
        }else{
            pred = new Prediction(trainingSet,null);
        }
        Long time = System.currentTimeMillis();
        //calculates prediction for every record in testSet
        int numberOfPredictions =0;

        for(Map.Entry<Integer, ArrayList<Integer>> entry : testSet.entrySet()){
            nextUser=entry.getKey();
            itemID=entry.getValue();
            if(type.equals("user")) {
                if (userID != nextUser) {
                    pred.setSimilarity(loadUserSimilarities(nextUser));
                }
            }
            for(int i=0; i<itemID.size();i++) {
                prediction = pred.total(nextUser, itemID.get(i),"user");
                if (userID == nextUser) {
                    predictions.get(userID).put(itemID.get(i), prediction);
                } else {
                    userID = nextUser;
                    predictions.put(userID, new HashMap<>());
                    predictions.get(userID).put(itemID.get(i), prediction);
                }
            }
            /*
            for(Integer item : itemID) {
                if(type.equals("item")){
                    pred.setSimilarity(loadItemSimilarities(item));
                }
                prediction = pred.total(nextUser, item, type);
                if (userID == nextUser) {
                    predictions.get(userID).put(item, prediction);
                } else {
                    userID = nextUser;
                    predictions.put(userID, new HashMap<>());
                    predictions.get(userID).put(item, prediction);
                }
            }*/

            if(numberOfPredictions%1000 ==0){
                System.out.println( numberOfPredictions + " Predictions");
                Long end = System.currentTimeMillis();
                System.out.println((end-time)/1000 + " seconds");
            }
            numberOfPredictions++;
        }
        Long end = System.currentTimeMillis();
        System.out.println((end-time)/1000 + "seconds");

       System.out.println("Predictions loaded and calculated");
    }


    //adds predictions from hashMap into database
    private void addPredictedRatings() {
        HashMap<Integer,Double> item_rating;
        int item_id;
        double rating;
        String sql;
        PreparedStatement stmt;
        try {
            sql = "UPDATE testTable SET prediction=? WHERE user_id=? AND item_id=?";
            stmt = c.prepareStatement(sql);

            for(int user: predictions.keySet()){
                item_rating = predictions.get(user);

                for(Map.Entry<Integer, Double> entry : item_rating.entrySet()) {
                    item_id = entry.getKey();
                    rating = entry.getValue();

                    stmt.setDouble(1, rating);
                    stmt.setInt(2, user);
                    stmt.setInt(3, item_id);

                    stmt.addBatch();
                }
            }
            stmt.executeBatch();
            c.commit();
            System.out.println("Predictions saved");
            stmt.close();
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
