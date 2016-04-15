package sa.edu.kaust.advos;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/* Code adapted from IntWritable */

@SuppressWarnings("unchecked")
public class IntTuple implements WritableComparable {
	private static int Shift_Lo3Bytes = 24;
	private static int Shift_Lo2Bytes = 16;
	private static int Shift_Lo1Bytes = 8;

	private static int Mask_Hi3Bytes = 255; // with logical '&' will masks the highest 8 bits
	private static int Mask_Hi2Bytes = 65535; // with logical '&' will masks the highest 16 bits


	private int value;

	public IntTuple() {}

	public IntTuple(int value) { set(value); }

	/** Set the value of this IntWritable. */
	public void set(int value) { this.value = value; }

	/** Return the value of this IntWritable. */
	public int get() { return value; }

	public void readFields(DataInput in) throws IOException {
		value = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(value);
	}

	public boolean equals(Object o) {
		if (!(o instanceof IntTuple))
			return false;
		IntTuple other = (IntTuple)o;
		return this.value == other.value;
	}

	public char getKey1() {
		return (char) (value >>> Shift_Lo3Bytes);
	}

	public char getKey2() {
		return (char) (( value >>> Shift_Lo2Bytes) & Mask_Hi3Bytes);
	}

	public int getKey3() {
		return (value & Mask_Hi2Bytes);
	}

	public int hashCode() { // keeps key 1 and 2 only
		return (value >>> Shift_Lo2Bytes);
	}

	public int compareTo(Object o) {
		int thisValue = this.value;
		int thatValue = ((IntTuple)o).value;
		return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
	}

	public String toString() {
		return Integer.toString(value);
	}

	public IntTuple(char key1, char key2, int w) {
		value = 0;
		value = (value | ((byte) key1));
		value = ((value << Shift_Lo1Bytes) | ((byte) key2));
		value = ((value << Shift_Lo2Bytes) | ((short) w));
	}

	public static void main(String... args) {
		IntTuple intT = new IntTuple('a', 'b', 0); // 97 98 0  =>  01100001 01100010 00000000 00000000
		System.out.println("intT.get(): " + intT.get());
		System.out.println("Key1: " + intT.getKey1());
		System.out.println("Key2: " + intT.getKey2());
		System.out.println("Key3: " + intT.getKey3());
	}

	public IntTuple clone() {
		return new IntTuple(value);
	}
	
	public String prettyPrint() {
		return "<" + getKey1() + "," + getKey2() + "," + getKey3() + ">";
	}


	/** A Comparator optimized for IntTuple. */ 
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(IntTuple.class);
		}

		public int compare(byte[] b1, int s1, int l1,
				byte[] b2, int s2, int l2) {
			int thisValue = readInt(b1, s1);
			int thatValue = readInt(b2, s2);
			return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		}
	}

	static { // register this comparator
		WritableComparator.define(IntTuple.class, new Comparator());
	}
}
