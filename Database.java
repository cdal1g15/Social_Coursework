
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
        db.sizeOfSet(db.trainingSet);
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


    public void sizeOfSet(HashMap inputSet){
        System.out.println(inputSet.size());
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

            //Checks if nextUser is CurrentUser, if so adds item to CurrentUsers hash, else moves on
            while(rs.next()){
                int nextUser= rs.getInt("user_id");

                if(user==nextUser) {
                    trainingSet.get(user).put(rs.getInt("item_id"), rs.getDouble("rating"));
                }else {
                    user = nextUser;
                    trainingSet.put(user, new HashMap<Integer,Double>());
                    trainingSet.get(user).put(rs.getInt("item_id"), rs.getDouble("rating"));
                }
            }
        }catch(SQLException e){
            System.out.println("SQLException: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
