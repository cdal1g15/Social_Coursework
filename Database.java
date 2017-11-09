
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class Database {


    private final String connection_string = "jdbc:sqlite:test.sl3";
    private Connection c;
    private static final String JDBC_DRIVER = "org.sqlite.JDBC";
    private HashMap<Integer, HashMap<Integer,Double>> trainingSet;
    private HashMap<Integer, Double> userAverages;
    //private static final int TEST_SET_SIZE = 60705;


    public static void main(String[] args) {
        Database db = new Database();
        //db.simCity();
        db.loadTrainingSet();//takes ~25 seconds
        db.loadUserAverages();
        db.sizeOfSet(db.trainingSet);
        db.storeUserSimilarity();
        //Similarity sim = new Similarity(db.trainingSet, db.userAverages);
        //sim.sumTotal(4, 135350);
        System.out.println(); //4 and 135350 have sim of 0.36501
    }


    //Initialise storage, connect to database
    private Database() {
        trainingSet = new HashMap<>();
        userAverages = new HashMap<>();

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


    private void sizeOfSet(HashMap inputSet){
        System.out.println(inputSet.size());
    }


    private void storeUserSimilarity(){
        Similarity sim = new Similarity(trainingSet, userAverages);
        ArrayList<ArrayList<Double>> simUsers = new ArrayList<>();
        for(int i=1; i<10/*userAverages.size()*/; i++){
            //store similarity somehow - possibly in sql?
        }
        System.out.println();
    }


    /**
     * Loads into Java, userAverages table
     */
    private void loadUserAverages(){
        try{
            String sql = "SELECT * FROM user_averages";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            while(rs.next()){
                userAverages.put(rs.getInt("user_id"),rs.getDouble("avg"));
            }
        } catch (SQLException e) {
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }


    //Loads trainingSet in java
    private void loadTrainingSet(){
        int user = 0;

        try{
            String sql = "SELECT * FROM trainingSet";
            PreparedStatement stmt = c.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
            c.commit();

            //Checks if nextUser is CurrentUser, if so adds item to CurrentUsers hash, else moves on
            while(rs.next()){
                int nextUser= rs.getInt("user_id");

                if(user==nextUser) {
                    trainingSet.get(user).put(rs.getInt("item_id"), rs.getDouble("rating"));
                }else {
                    user = nextUser;
                    trainingSet.put(user, new HashMap<>());
                    trainingSet.get(user).put(rs.getInt("item_id"), rs.getDouble("rating"));
                }
            }
        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
