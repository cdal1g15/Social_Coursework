
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Database {


    private final String connection_string = "jdbc:sqlite:/media/conor/AdrianaTheSlut/test.sl3";
    private Connection c;
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private HashMap<Integer, HashMap<Integer, Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    private HashMap<Integer, HashMap<Integer, Double>> predictions;
    private ArrayList<Integer> testUsers;
    private HashMap<Integer,Integer> testSet;
    private HashMap<Integer, HashMap<Integer, Double>> similarities;
    private String[] sqlSim;
    //private static final int TEST_SET_SIZE = 60705;


    public static void main(String[] args) {
        Database db = new Database();
        db.loadTrainingSet();//takes ~25 seconds
        db.loadUserAverages();
        db.loadUniqueTestUsers();
        db.loadTestSet();
        db.sizeOfSet(db.trainingSet);
        db.storePredictions();
        db.addPredictedRatings();
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


    private void storeUserSimilarity() {
        Similarity sim = new Similarity(trainingSet, userAverages);
        HashMap<Integer, HashMap<Integer, Double>> simUsers = new HashMap<>();
        String sql;
        PreparedStatement stmt = null;
        int user1;
        sql = "INSERT INTO simMatrix VALUES (?,?,?)";
        try {
            stmt = c.prepareStatement(sql);
            Long time = System.currentTimeMillis();
            for (int i = 0; i < testUsers.size(); i++) {
                user1 = testUsers.get(i);
                int j=1;
                int limit =0;
                while( j < userAverages.size() && limit <120) {
                    Double userSim = sim.sumTotal(user1, j);
                    if(userSim > 0.7 && userSim <1) {
                        stmt.setInt(1, user1);
                        stmt.setInt(2, j);
                        stmt.setDouble(3, userSim);
                        stmt.addBatch();
                        limit++;
                    }
                    j++;
                }
                System.out.println("Done " + user1);
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

    private void loadTestSet() {
        try {
            String sql = "SELECT  user_id,item_id FROM testTable";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while (rs.next()) {
                testSet.put(rs.getInt("user_id"), rs.getInt("item_id"));
            }
            System.out.println("Test Set loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void loadUniqueTestUsers() {
        try {
            String sql = "SELECT DISTINCT user_id FROM testTable";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while (rs.next()) {
                testUsers.add(rs.getInt("user_id"));
            }
            System.out.println("Test Users loaded.");
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }


    //Loads trainingSet in java
    private void loadTrainingSet() {
        int user = 0;
        int nextUser = 0;

        try {
            String sql = "SELECT * FROM trainingSet";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            //Checks if nextUser is CurrentUser, if so adds item to CurrentUsers hash, else moves on
            while (rs.next()) {
                nextUser = rs.getInt("user_id");

                if (user == nextUser) {
                    trainingSet.get(user).put(rs.getInt("item_id"), rs.getDouble("rating"));
                } else {
                    user = nextUser;
                    trainingSet.put(user, new HashMap<>());
                    trainingSet.get(user).put(rs.getInt("item_id"), rs.getDouble("rating"));
                }
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Training Set loaded.");
    }

    //stores predictions calculated in predictions hash map
    private void storePredictions(){
        int userID = 0;
        int nextUser=0;
        int itemID=0;
        double prediction = 0.0;
        Prediction pred = new Prediction(trainingSet,userAverages,loadSimilarities(1));

        //calculates prediction for every record in testSet
        for(Map.Entry<Integer, Integer> entry : testSet.entrySet()){
            nextUser=entry.getKey();
            itemID=entry.getValue();

            //won't load again if user is the same
            if(userID!=nextUser) {
                similarities = loadSimilarities(nextUser);
                pred = new Prediction(trainingSet,userAverages,similarities);
            }

            prediction = pred.total(nextUser, itemID);

            if(userID == nextUser){
                predictions.get(userID).put(itemID, prediction);
            }else{
                userID=nextUser;
                predictions.put(userID, new HashMap<Integer, Double>());
                predictions.get(userID).put(itemID, prediction);
            }
        }

       System.out.println("Predictions loaded and calculated");
    }


    //adds predictions from hashMap into database
    private void addPredictedRatings() {

        HashMap<Integer,Double> item_rating;
        int item_id;
        double rating;
        String sql;
        PreparedStatement stmt = null;


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

            stmt.close();
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }

    //loads from database the similarity measures for a user

    public HashMap<Integer, HashMap<Integer, Double>> loadSimilarities(int test_user){

        int user = 0;
        int nextUser;

        HashMap<Integer, HashMap<Integer, Double>> similarities = new HashMap<Integer, HashMap<Integer, Double>>();


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
                    similarities.get(user).put(rs.getInt("user_2"), rs.getDouble("sim"));
                } else {
                    user = nextUser;
                    similarities.put(user, new HashMap<>());
                    similarities.get(user).put(rs.getInt("user_2"), rs.getDouble("sim"));
                }
            }


        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }


        return similarities;
    }

}
