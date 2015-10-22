package heap;

import java.util.Arrays;

public class Tuple {

	protected byte[] data;
	
	public Tuple(byte[] byteArray, int start, int size) {
		data = Arrays.copyOfRange(byteArray, start, size);
	}

	public Tuple() {
		data = null;
	}

	public int getLength() {
		return data.length;
	}

	public byte[] getTupleByteArray() {
		return data;
	}

}
