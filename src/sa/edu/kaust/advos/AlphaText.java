package sa.edu.kaust.advos;

import org.apache.hadoop.io.Text;

public class AlphaText extends Text {
	private String temp;
	private static int a = (int) 'a';
	private static int z = (int) 'z';

	@Override
	public int hashCode() {
		// System.err.println("hashCode: " + (this.charAt(0) - a)); // testing that this function is being called.
		return (this.charAt(0) - a); // values will be 0 for a through 25 for z
	}

	public void toLowerCase() {
		temp = this.toString(); // Sadly - this is a String creation point!!!
		this.set(temp.toLowerCase());
	}

	public boolean startsWithAlpha() {
		return (this.charAt(0) <= z && this.charAt(0) >= a);
	}
	
	public static AlphaText createInLowerCase(String s) {
		return new AlphaText(s.toLowerCase());
	}

	public AlphaText() {
		super();
	}
	
	public AlphaText(String s) {
		super(s);
	}
}
