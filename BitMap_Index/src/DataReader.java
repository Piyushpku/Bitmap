import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * 
 */

/**
 * @author ekjot
 *
 */
public class DataReader {

	private BufferedReader br;
	static long io = 0L;

	DataReader(String fileAddress) throws IOException {
		br = new BufferedReader(new FileReader(fileAddress));
	
	}

	DataReader() {
	}

	byte readByte() throws IOException {
		io++;
		return (byte) br.read();
	}

	String readLine() throws IOException {

		return br.readLine();
	}

	Tuple readTuple(byte firstByte) throws IOException {// firstByte is already read byte

		// new data
		byte ind = 0;
		byte[] data = new byte[100];

		// reading tuple
		while (ind < 100) {
			data[ind++] = firstByte;
			firstByte = readByte();

		}
		// now stream is on return key ie. 13
		// firstByte=readByte();

		// now stream is on new line char ie. 10

		return new Tuple(data);
	}

	void delink() throws IOException {
		br.close();
	}

	public Tuple readTupleAt(int index) throws IOException {

		br.skip((index - 1) * 101);
		return readTuple(readByte());
	}

}
