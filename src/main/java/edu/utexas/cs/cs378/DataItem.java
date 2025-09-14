package edu.utexas.cs.cs378;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * This class represents a data item with a string and two float values.
 * 
 * @author kiat
 *
 */
public class DataItem {

	private String line;
	private float valueA;
	private float valueB;

	public DataItem() {

	}

	public DataItem(String line, float valueA, float valueB) {
		super();
		this.line = line;
		this.valueA = valueA;
		this.valueB = valueB;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public float getValueA() {
		return valueA;
	}

	public void setValueA(float valueA) {
		this.valueA = valueA;
	}

	public float getValueB() {
		return valueB;
	}

	public void setValueB(float valueB) {
		this.valueB = valueB;
	}

	/**
	 * This method manually serializes a data object of this type into a byte array.
	 * Order of writing the data into byte array matters and should be de-serialized
	 * in the same order.
	 * 
	 * @return
	 */
	public byte[] handSerializationWithByteBuffer() {

		byte[] lineBytes = line.getBytes(Charset.forName("UTF-8"));
		// 8 bytes for each float number (two float numbers 16)
		// 4 byte for an integer to write the length of the string
		// lineBytes.length for the legth of the string.

		ByteBuffer byteBuffer = ByteBuffer.allocate(2 * 8 + 4 + lineBytes.length);

		// 1. String line
		// First length of it and then its bytes
		// Each string value might be of different size. We have to write down its
		// length.
		byteBuffer.putInt(lineBytes.length);
		byteBuffer.put(lineBytes);

		// 2. ValueA
		byteBuffer.putFloat(valueA);

		// 3. ValueB
		byteBuffer.putFloat(valueB);

		return byteBuffer.array();
	}

	/**
	 * This method manually de-serializes an object of DataItem from a given byte
	 * array.
	 * 
	 * @param buf
	 * @return
	 */
	public DataItem deserializeFromBytes(byte[] buf) {

		ByteBuffer byteBuffer = ByteBuffer.wrap(buf);

		// 1. Read the line string back from the byte array.

		int stringSize = byteBuffer.getInt(); // 4 bytes
		String tmpLine = extractString(byteBuffer, stringSize);

		// 2. read a float from the given byte array
		float valueATemp = byteBuffer.getFloat();

		// 3. read the last float byte array back.
		float valueBTemp = byteBuffer.getFloat();

		return new DataItem(tmpLine, valueATemp, valueBTemp);

	}

	/**
	 * This method reads a string from a buteBuffer.
	 * 
	 * @param byteBuffer
	 * @param stringSize
	 * @return
	 */
	String extractString(ByteBuffer byteBuffer, int stringSize) {
		byte[] stringBytes = new byte[stringSize];
		byteBuffer.get(stringBytes, 0, stringSize);

		String mystring = new String(stringBytes, Charset.forName("UTF-8"));
		return mystring;
	}

	@Override
	public String toString() {
		return "DataItem [line=" + line + ", valueA=" + valueA + ", valueB=" + valueB + "]";
	}

}
