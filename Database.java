
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Database {


    private final String connection_string = "jdbc:sqlite:test.sl3";
    private Connection c;
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private HashMap<Integer, HashMap<Integer, Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    private HashMap<Integer, HashMap<Integer, Double>> predictions;
    private ArrayList<Integer> testUsers;
    private ArrayList<Integer> testItems;
    private HashMap<Integer,ArrayList<Integer>> testSet;
    private HashMap<Integer, HashMap<Integer, Double>> similarities;
    private String[] sqlSim;
    //private static final int TEST_SET_SIZE = 60705;


    public static void main(String[] args) {
        Database db = new Database();
        db.loadTrainingSet("item");//takes ~25 seconds
                                        //different hashmap structure to user collab filtering
        db.loadUserAverages();

        db.loadUniqueTest("item");

        //db.loadUniqueTestUsers();

        db.loadTestSet();
        db.sizeOfSet(db.trainingSet);

        db.storeSimilarity("item");

        //db.storeSimilarity("user");
        //db.storePredictions();
        //db.addPredictedRatings();
        //Similarity sim = new Similarity(db.trainingSet, db.userAverages);
        //sim.sumTotal(4, 135350);
        System.out.println(); //4 and 135350 have sim of 0.36501
    }


    //Initialise storage, connect to database
    private Database() {
        trainingSet = new HashMap<>();
        userAverages = new HashMap<>();
        predictions = new HashMap<>();
        testUsers = new ArrayList<>();
        testItems = new ArrayList<>();
        testSet = new HashMap<>();
        similarities = new HashMap<>();

        try {
            Class.forName(JDBC_DRIVER);
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
                while( j < noOfElements && limit <400) {
                    Double similarity = sim.sumTotal(value, j, field);
                    if(similarity > 0.7 && similarity <1) {
                        stmt.setInt(1, value);
                        stmt.setInt(2, j);
                        stmt.setDouble(3, similarity);
                        stmt.addBatch();
                        limit++;
                    }
                    j++;
                }
                Long end = System.currentTimeMillis();
                System.out.println((end-time)/1000 + "seconds");
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
                userAverages.put(rs.getInt("user_id"), rs.getDouble("average"));
            }
            System.out.println("User Averages loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadTestSet() {
        try {
            int user = 0;
            String sql = "SELECT  user_id,item_id FROM testTable";
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

        String columnName = field +"_id";
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
            String sql = "SELECT * FROM trainingSet";
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
    }


    //loads from database the similarity measures for a user
    public HashMap<Integer, HashMap<Integer, Double>> loadSimilarities(int test_user){
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


    //stores predictions calculated in predictions hash map
    private void storePredictions(){
        int userID = 0;
        int nextUser;
        ArrayList<Integer> itemID;
        double prediction;
        Prediction pred = new Prediction(trainingSet,userAverages,null);
        Long time = System.currentTimeMillis();
        //calculates prediction for every record in testSet
        int adrianaIsASlut =0;
        for(Map.Entry<Integer, ArrayList<Integer>> entry : testSet.entrySet()){
            nextUser=entry.getKey();
            itemID=entry.getValue();
            if (userID != nextUser) {
                pred.setSimilarity(loadSimilarities(nextUser));
            }

            for(int i=0; i<itemID.size();i++) {
                prediction = pred.total(nextUser, itemID.get(i));
                if (userID == nextUser) {
                    predictions.get(userID).put(itemID.get(i), prediction);
                } else {
                    userID = nextUser;
                    predictions.put(userID, new HashMap<>());
                    predictions.get(userID).put(itemID.get(i), prediction);
                }
            }

            if(adrianaIsASlut%1000 ==0){
                System.out.println( adrianaIsASlut + " Predictions");
                Long end = System.currentTimeMillis();
                System.out.println((end-time)/1000 + " seconds");
            }
            adrianaIsASlut++;
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
