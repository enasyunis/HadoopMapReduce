package sa.edu.kaust.advos;

import org.apache.hadoop.io.Text;

public class ConcatText extends Text {

	public ConcatText() {
		super();
	}
	
	public ConcatText(String s) {
		super(s.replaceAll(" \t", ""));
	}
}
