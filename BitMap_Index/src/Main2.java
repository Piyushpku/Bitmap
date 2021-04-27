import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author ekjot
 *
 */
public class Main2 {

	static int TOTAL_TUPLES;
	static int SUBLIST_SIZE = 30000;
	static int MAX_TUPLES = 100000;

	static String f1 = "Data1";
	static String f2 = "Data2";

	static boolean WRITE_UNCOMPRESSED = true;
	static boolean WRITE_COMPRESSED = true;
	static boolean WRITE_MERGED = true;

	static String fileAddress1;
	static String fileAddress2;

	static int tuplesInFile1;

	/*
	 * ALGORITHM (TPMMS to build Bitmap Index):
	 * 
	 * 1. Create sorted sublists of maximum size that main memory can accommodate.
	 * *sublist should contain key and its index in provided original file 2.
	 * 
	 */

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		System.out.println("File 1:");
		fileAddress1 = "C:\\Users\\ekjot\\git\\BitMap_Index\\BitMap_Index\\Data\\Data1.txt"; // getFileAddress();

		System.out.println("\nFile 2:");
		fileAddress2 = "C:\\Users\\ekjot\\git\\BitMap_Index\\BitMap_Index\\Data\\Data2.txt";// getFileAddress();

		/*
		 * C:\Users\ekjot\git\BitMap_Index\BitMap_Index\Data\Data.txt
		 */

		Double start = (double) System.nanoTime();

		int numOfTuples = 0;
		boolean file1Complete = false, file2Complete = false;

		DataReader dr = new DataReader(fileAddress1);
		byte firstByte = dr.readByte();

		HashMap<Integer, ArrayList<Integer>> empHash = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, ArrayList<Integer>> genderHash = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, ArrayList<Integer>> deptHash = new HashMap<Integer, ArrayList<Integer>>();

		// run until both files are complete
		while (!file1Complete || !file2Complete) {

			// code to select second file when first file ends
			if (firstByte == -1) { // any file ends

				if (!file1Complete) { // if first file ended
					file1Complete = true;

					tuplesInFile1 = numOfTuples;

					dr = new DataReader(fileAddress2);
					firstByte = dr.readByte();

					// If the second file is empty
					if (firstByte == -1) {
						file2Complete = true;
						break;
					}
				} else if (!file2Complete) { // if second file ended
					file2Complete = true;
					break;
				}
			}

			// code to read a tuple and extract values
			Tuple tuple = dr.readTuple(firstByte);
			numOfTuples++;

			if (numOfTuples == MAX_TUPLES) {
				break;
			}
			int empid = tuple.getEmpIDAsNum();
			int gender = tuple.getGenderAsNum();
			int dept = tuple.getDeptAsNum();

			addToMap(empid, numOfTuples, empHash);
			addToMap(gender, numOfTuples, genderHash);
			addToMap(dept, numOfTuples, deptHash);

			firstByte = dr.readByte();
		}
		TOTAL_TUPLES = numOfTuples;
		makeBitmapIndex(empHash, 'E');
		makeBitmapIndex(deptHash, 'D');
		makeBitmapIndex(genderHash, 'G');

		if (WRITE_MERGED) {
			File f = new File("merged.txt");
			if (f.exists()) {
				f.delete();
			}
			writeMergedFile();
		}
		System.out.println("\nTotal time : " + (System.nanoTime() - start) / 1000000000 + " sec");

		writeOutput();

	}

	private static void writeOutput() {
		if (WRITE_UNCOMPRESSED) {
			System.out.println("\nEmpID Uncompressed: " + (new File("E.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Dept Uncompressed: " + (new File("D.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Gender Uncompressed: " + (new File("G.txt").length() * 1.0 / 1024) + " KB");
		}
		if (WRITE_COMPRESSED) {
			System.out.println("\nEmpID Compressed: " + (new File("E_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Dept Compressed: " + (new File("D_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Gender Compressed: " + (new File("G_compressed.txt").length() * 1.0 / 1024) + " KB");
		}
		if (WRITE_MERGED) {
			System.out.println("\nMerged: " + (new File("merged.txt").length() * 1.0 / 1024) + " KB");
		}

	}

	private static void addToMap(int key, int index, HashMap<Integer, ArrayList<Integer>> map) {
		if (!map.containsKey(key)) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			list.add(index);
			map.put(key, list);
		} // if treemap have given key, add index to its arraylist
		else {
			map.get(key).add(index);

		}

	}

	private static void makeBitmapIndex(HashMap<Integer, ArrayList<Integer>> map, char col) throws IOException {
		ArrayList<Integer> keys = new ArrayList<Integer>(map.keySet());
		Collections.sort(keys);

		// stream for writing uncompressed indexes
		BufferedWriter bwu = new BufferedWriter(new FileWriter(col + ".txt"));

		// stream for writing compressed indexes
		BufferedWriter bwc = new BufferedWriter(new FileWriter(col + "_compressed.txt"));

		if (!WRITE_UNCOMPRESSED) {
			//bwu.nullWriter();
		}
		if (!WRITE_COMPRESSED) {
			//bwc.nullWriter();
		}

		for (int key : keys) {

			writeBIForKey(bwu, bwc, map, key, col);

		}

		bwu.close();
		bwc.close();
	}

	private static void writeBIForKey(BufferedWriter bwu, BufferedWriter bwc, HashMap<Integer, ArrayList<Integer>> map,
			int key, char col) throws IOException {

		ArrayList<Integer> index = map.get(key);

		Collections.sort(index);

		writeBitmapIndex(bwu, bwc, key, index.toArray(new Integer[index.size()]), col);
		map.remove(key);

	}

	private static void writeMergedFile() throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("E.txt"));
		// stream for writing merged tuples
		BufferedWriter bwm = new BufferedWriter(new FileWriter("merged.txt", true));
		DataReader dr = null;
		String s = br.readLine();
		while (s!=null) {

			String str = (s.split(" "))[1];
			
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();

			for (int i = 0; i < str.length(); i++) {
				int index = i + 1;
				if (str.charAt(i) == '1') {
					if (index <= tuplesInFile1) {
						dr = new DataReader(fileAddress1);
					} else {
						dr = new DataReader(fileAddress2);
						index -= tuplesInFile1;
					}
					tuples.add(dr.readTupleAt(index));
				}
			}
			
			int latestUpdate=0;
			Tuple latestTuple = null;
			for(Tuple t:tuples) {
				if(latestUpdate<t.getLastUpdateAsNum()) {
					latestUpdate=t.getLastUpdateAsNum();
					latestTuple=t;
				}
			}
			
			bwm.write(latestTuple.toString() + "\r");
			dr.delink();
			s=br.readLine();
		}
		
		

		
		
		bwm.close();
		// System.out.print(t.toString()+"\n");

	}

	private static void writeBitmapIndex(BufferedWriter bwu, BufferedWriter bwc, Integer key, Integer[] index, char col)
			throws IOException {
		String Key = "" + key;
		if (col == 'E') {
			Key = String.format("%08d", key);
		} else if (col == 'D') {
			Key = String.format("%03d", key);
		}

		bwu.write(Key + " ");
		bwc.write(Key + " ");
		int i = 1;

		for (int ind : index) {

			// for uncompressed
			for (; i < ind; i++) {
				bwu.write("0");
			}
			bwu.write("1");
			// System.out.println(key+"-"+ind);
			i++;

			// for compressed
			String bin = Integer.toBinaryString(ind);
			for (int j = 1; j < bin.length(); j++) {
				bwc.write("1");
			}
			bwc.write("0" + bin);

		}
		while (TOTAL_TUPLES - (i++) + 1 > 0) {
			bwu.write("0");
		}
		bwu.write("\n");
		bwc.write("\n");
	}

	private static String getFileAddress() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("Enter File Address: ");
		String address = (new BufferedReader(new InputStreamReader(System.in))).readLine();
		if (!checkFileValidity(address)) {
			System.out.println("Entered File Address is invalid.\nTry again.");
			address = getFileAddress();
		}

		return address;
	}

	private static boolean checkFileValidity(String fileAddress) {
		File f = new File(fileAddress);
		return f.exists() && !f.isDirectory();
	}

}
