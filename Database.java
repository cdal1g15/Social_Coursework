
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class Database {


    final String connection_string = "jdbc:sqlite:test.sl3";
    public Connection c;
    static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private ArrayList<Integer> simItems;
    private ArrayList<Double> ratings;
    private ArrayList<Double> ratings2;

    private HashMap<Integer, HashMap<Integer,Double>> trainingSet;

    private User userPredicting;

    static final int TEST_SET_SIZE = 60705;

    public static void main(String[] args) {
        Database db = new Database();
        //db.simCity();
        db.loadTrainingSet();
        System.out.println(db.trainingSet.size());
    }



    //Initialise storage, connect to database
    public Database() {
        simItems = new ArrayList<Integer>();
        ratings = new ArrayList<Double>();
        ratings2 = new ArrayList<Double>();

        trainingSet = new HashMap<Integer, HashMap<Integer,Double>>();

        try {
            Class.forName(JDBC_DRIVER);
            c = DriverManager.getConnection(connection_string);
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void setPragmaValues(){
        try{

            PreparedStatement stmt = c.prepareStatement("PRAGMA TEMP_STORE = MEMORY");
            stmt.execute();
            stmt = c.prepareStatement("PRAGMA SYNCHRONOUS = 0");
            stmt.execute();
            stmt = c.prepareStatement("PRAGMA JOURNAL_MODE = MEMORY");
            stmt.execute();
            stmt = c.prepareStatement("PRAGMA LOCKING_MODE = EXCLUSIVE");
            stmt.execute();

            stmt.close();


        }catch(SQLException e){
            e.printStackTrace();
        }
    }


    //Loads trainingSet in java
    public void loadTrainingSet(){

        int user = 0;
        int i=0;

        try{
            String sql = "SELECT * FROM trainingSet";
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();


            while(rs.next()){
                int nextUser= rs.getInt(1);
                if(user==nextUser) {
                    //Add items to hashmap for user
                    //trainingSet.get(user).put(itemResultSet.getInt(1), itemResultSet.getDouble(2));
                }else {
                    user = nextUser;
                }
                trainingSet.put(user, new HashMap<Integer,Double>());
                /*
                sql="SELECT item_id,rating FROM trainingSet where user_id=" + user;
                stmt = c.prepareStatement(sql);

                //ResultSet itemResultSet = stmt.executeQuery();
                c.commit();

                while(itemResultSet.next())

                i++;*/
                /*if(i%5000==0){
                    System.out.println(i);
                }*/
            }

        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }

    }


    /**
     * Takes two users in and returns a list of similar items
     * @param user1
     * @param user2
     * @return
     */
    public ArrayList<Integer> getSimItems(int user1, int user2){


        ArrayList<Integer> simItems = new ArrayList<Integer>();
        String selectStmt = "SELECT item_id FROM trainingSet WHERE user_id=";

        try{

            String sql = selectStmt + user1 + " INTERSECT " + selectStmt + user2;
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();

            while(rs.next())
               simItems.add(rs.getInt(1));

            stmt.close();
            rs.close();

        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }

        return simItems;
    }


    /**
     * Returns arraylist of ratings given the user and list of items
     * @param user_id
     * @param simItems
     * @return
     */
    public ArrayList<Double> getRatings(int user_id, ArrayList<Integer> simItems){

        ArrayList<Double> ratings = new ArrayList<Double>();

        String sql = "SELECT rating from trainingSet where user_id=" + user_id + " and (";
        for(int b=0; b<simItems.size()-1; b++){
            sql+= " item_id=" + simItems.get(b) + " OR";
        }
        sql+= " item_id=" + simItems.get(simItems.size()-1) + ") ORDER BY item_id";

        try{
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();

            while(rs.next())
                ratings.add(rs.getDouble(1));

            stmt.close();
            rs.close();

        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }

        return ratings;

    }



    public double getRating(int user_id, int item_id){

        double rating = 0.0;

        String sql = "SELECT rating from trainingSet where user_id=" + user_id + " AND item_id=" + item_id;

        try{
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();

            rating = rs.getDouble(1);

            stmt.close();
            rs.close();
            return rating;
        }catch(SQLException e){
            return 0;
        }
    }



    public void insertPredictedRating(int user_id, double rating){
        try{
            String sql = "UPDATE testTable SET prediction=? WHERE user_id=?";
            PreparedStatement stmt = c.prepareStatement(sql);

            stmt.setDouble(1,rating);
            stmt.setInt(2, user_id);

            stmt.executeUpdate();
            c.commit();

            stmt.close();
        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }



    public double getAverage(int user_id){

        double avg = 0.0;

        try{
            String sql="SELECT avg from user_averages WHERE user_id =" + user_id;
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();

            avg= rs.getDouble(1);

            stmt.close();
            rs.close();
        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }

        return avg;
    }




    public double getTopSum(int user1, int user2){
        double sum = 0.0;
        if(simItems.isEmpty())
            return 0.0;

        ratings = getRatings(user1, simItems);// move out of scope
        ratings2 = getRatings(user2, simItems);


        double user1Avg = getAverage(user1);
        double user2Avg = getAverage(user2);

        for(int i=0; i < ratings.size(); i++){
            sum += (ratings.get(i) - user1Avg) * (ratings2.get(i) - user2Avg);
        }

        return sum;
    }



    public double getSumBooty(int user1, int user2){
        double booty = 0.0;

        double leftBooty = 0.0;
        double rightBooty = 0.0;

        double user1Avg = getAverage(user1);
        double user2Avg = getAverage(user2);



        for(int i=0; i < ratings.size(); i++){
            leftBooty += Math.pow((ratings.get(i) - user1Avg),2);
            rightBooty += Math.pow((ratings2.get(i) - user2Avg),2); ;
        }

        booty = Math.sqrt(leftBooty) * Math.sqrt(rightBooty);

        return booty;
    }



    public void simCity(){

        int[] user_ids;
        int[] item_ids;
        double threshold = 0.7;
        int limit = 0;
        double value = 0;
        ArrayList<User> simMeasures = new ArrayList<User>();

        user_ids=getAllUserIDs();
        item_ids=getAllItemIDs();

        for(int i = 1; i < 60706; i++){ //for every user in test set 60706

            userPredicting = new User(user_ids[i-1],getAverage(user_ids[i-1]));

            int j = 1;

            while(limit < 5 && j < 16045652){ //for first 30 suitable comparisons
                if(j!=user_ids[i-1] && getRating(j,item_ids[i-1])!=0){
                    value = getSimilarity(user_ids[i-1],j);
                    System.out.println("User: " + j);
                    if(value > threshold){
                        //add to list of similarity measures up to 30
                        simMeasures.add(new User(j,getRating(j,item_ids[i-1]),getAverage(j),value));
                        limit++;
                        System.out.println((5 - limit) + " away from threshold");
                    }
                }
                j++;
            }

            insertPredictedRating(user_ids[i-1],predictRating(userPredicting, simMeasures));
            simMeasures.clear();
            limit=0;
        }

    }



    private double predictRating(User user, ArrayList<User> simMeasures) {
        double prediction = userPredicting.getAverage();
        double topSum = 0.0;
        double bottomSum = 0.0;


        for(User u : simMeasures){
            topSum+= (u.getSimMeasure() * (u.getRating() - u.getAverage()));
            bottomSum += u.getSimMeasure();
        }

        prediction += (topSum / bottomSum);

        System.out.println("Prediction: " + prediction);
        return prediction;

    }



    public double getSimilarity(int user1, int user2){

        simItems = getSimItems(user1, user2);
        double topSum = getTopSum(user1, user2);
        simItems.clear();

        if(topSum > 0){
            return (topSum/getSumBooty(user1, user2));
        }

        return 0.0;

    }



    public int[] getAllUserIDs(){
        int[] userIDs = new int[TEST_SET_SIZE];

        try{
            String sql="SELECT user_id FROM testTable";
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();
            for(int i = 0; rs.next(); i++){
                userIDs[i] = rs.getInt(1);
            }

            stmt.close();
            rs.close();

            return userIDs;
        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }

        return userIDs;
    }




    public int[] getAllItemIDs(){
        int[] itemIDs = new int[TEST_SET_SIZE];

        try{
            String sql="SELECT item_id FROM testTable";
            PreparedStatement stmt = c.prepareStatement(sql);

            ResultSet rs = stmt.executeQuery();
            c.commit();
            for(int i = 0; rs.next(); i++){
                itemIDs[i] = rs.getInt(1);
            }

            stmt.close();
            rs.close();


            return itemIDs;
        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }

        return itemIDs;
    }

}
