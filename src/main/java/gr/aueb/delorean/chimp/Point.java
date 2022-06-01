package gr.aueb.delorean.chimp;

public class Point {

	private final long timestamp;
	private final float value;
	
	public Point(long timestamp, float value) {
		this.timestamp = timestamp;
		this.value = value;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public float getValue() {
		return value;
	}
	
}
