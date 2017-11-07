
public class User {
	
	private int user_id;
	private double simMeasure;
	private double rating;
	private double average;
	
	public User(int user_id,double average){
		this.user_id=user_id;
		this.average=average;
		simMeasure=0;
	}
	
	public User(int user_id, double rating, double average, double simMeasure){
		this.user_id=user_id;
		this.rating=rating;
		this.average=average;
		this.simMeasure=simMeasure;
	}
	
	public int getUser_id() {
		return user_id;
	}

	public double getSimMeasure() {
		return simMeasure;
	}

	public double getRating() {
		return rating;
	}

	public double getAverage() {
		return average;
	}
}
