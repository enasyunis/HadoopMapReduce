package sa.edu.kaust.advos;

public class TestMe {
	public static void main(String... args) {
		String s = "HiMe"; // 72 105 77 101
		int len = s.length();
		len = len>4?4:len; // do not go above 4 bytes
		int mine =0;
		for (int i =0; i < len; i++) {
			mine = (mine << 8) | ((byte) s.charAt(i));
			System.out.println(mine);
		}
		
		System.out.println("mine is: " + mine);
		//StringBuilder sb = new StringBuilder();
		
		// 01001000 01101001 01001101 01100101
		System.out.println((char) (mine >>> 24 ) );
		System.out.println((char) ((mine >>> 16 ) & 255));
		System.out.println((char) ((mine >>> 8  ) & 255));
		System.out.println((char) (mine & 255));
		/*
		for (int i=3; i>=0; i--) {
			System.out.println(( (mine >> (8*i)) & (2^8-1) ));
			
			//sb.append((byte) ( (mine >> (8*(3-i))) & (2^8-1) )  );
		}
		*/
		//System.out.println("string is: " + sb.toString());
		
		
	}
	
	
}
