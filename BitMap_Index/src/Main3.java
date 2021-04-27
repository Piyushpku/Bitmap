
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
public class Main3 {

	static int TOTAL_TUPLES;
	static int SUBLIST_SIZE = 30000;
	static int MAX_TUPLES = 100000;

	static String f1 = "Data1";
	static String f2 = "Data2";

	static boolean WRITE_UNCOMPRESSED = true;
	static boolean WRITE_COMPRESSED = true;
	static boolean WRITE_MERGED = false;

	static String fileAddress1;
	static String fileAddress2;

	static int tuplesInFile1;
	static double indexTime = 0;

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
		fileAddress1 = getFileAddress(); // "C:\\Users\\Piyush\\Desktop\\Data\\Data1.txt";

		System.out.println("\nFile 2:");
		fileAddress2 = getFileAddress(); // "C:\\Users\\Piyush\\Desktop\\Data\\Data2.txt";

		/*
		 * C:\Users\ekjot\git\BitMap_Index\BitMap_Index\Data\Data.txt
		 */

		Double start = (double) System.nanoTime();

		createMap(fileAddress1);
		createMap(fileAddress2);

		if (WRITE_MERGED) {
			File f = new File("merged.txt");
			if (f.exists()) {
				f.delete();
			}
			writeMergedFile();
		}
		System.out.println("\nIndexing time : " + indexTime / 1000000000 + " sec");
		System.out.println("File creation time : " + (System.nanoTime() - start - indexTime) / 1000000000 + " sec");

		System.out.println("Total time : " + (System.nanoTime() - start) / 1000000000 + " sec");

		writeOutput();

	}

	public static void createMap(String fileAddress) throws IOException {
		int numOfTuples = 0;
		DataReader dr = new DataReader(fileAddress);
		byte firstByte = dr.readByte();

		HashMap<Integer, ArrayList<Integer>> empHash = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, ArrayList<Integer>> genderHash = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, ArrayList<Integer>> deptHash = new HashMap<Integer, ArrayList<Integer>>();

		double time = System.nanoTime();
		// run until both files are complete
		while (firstByte != -1) {

			// code to read a tuple and extract values
			Tuple tuple = dr.readTuple(firstByte);
			// System.out.println(tuple.toString());
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
		indexTime += System.nanoTime() - time;
		TOTAL_TUPLES = numOfTuples;
		String e = null, d = null, g = null;
		if (fileAddress.equals(fileAddress1)) {
			e = "E1";
			d = "D1";
			g = "G1";
		} else if (fileAddress.equals(fileAddress2)) {
			e = "E2";
			d = "D2";
			g = "G2";
		}
		makeBitmapIndex(empHash, e);
		makeBitmapIndex(deptHash, d);
		makeBitmapIndex(genderHash, g);

	}

	private static void writeOutput() {
		if (WRITE_UNCOMPRESSED) {
			System.out.println("\nFor file1");
			System.out.println("EmpID Uncompressed: " + (new File("E1.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Dept Uncompressed: " + (new File("D1.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Gender Uncompressed: " + (new File("G1.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("\nFor file2");
			System.out.println("EmpID Uncompressed: " + (new File("E2.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Dept Uncompressed: " + (new File("D2.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Gender Uncompressed: " + (new File("G2.txt").length() * 1.0 / 1024) + " KB");
		}
		if (WRITE_COMPRESSED) {
			System.out.println("\nFor file1");
			System.out.println("EmpID Compressed: " + (new File("E1_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Dept Compressed: " + (new File("D1_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Gender Compressed: " + (new File("G1_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("\nFor file2");
			System.out.println("EmpID Compressed: " + (new File("E2_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Dept Compressed: " + (new File("D2_compressed.txt").length() * 1.0 / 1024) + " KB");
			System.out.println("Gender Compressed: " + (new File("G2_compressed.txt").length() * 1.0 / 1024) + " KB");
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

	private static void makeBitmapIndex(HashMap<Integer, ArrayList<Integer>> map, String col) throws IOException {
		ArrayList<Integer> keys = new ArrayList<Integer>(map.keySet());
		Collections.sort(keys);

		// stream for writing uncompressed indexes
		BufferedWriter bwu = new BufferedWriter(new FileWriter(col + ".txt"));

		// stream for writing compressed indexes
		BufferedWriter bwc = new BufferedWriter(new FileWriter(col + "_compressed.txt"));

		if (!WRITE_UNCOMPRESSED) {
			// bwu.nullWriter();
		}
		if (!WRITE_COMPRESSED) {
			// bwc.nullWriter();
		}

		for (int key : keys) {

			writeBIForKey(bwu, bwc, map, key, col);

		}

		bwu.close();
		bwc.close();
	}

	private static void writeBIForKey(BufferedWriter bwu, BufferedWriter bwc, HashMap<Integer, ArrayList<Integer>> map,
			int key, String col) throws IOException {

		ArrayList<Integer> index = map.get(key);

		Collections.sort(index);

		writeBitmapIndex(bwu, bwc, key, index.toArray(new Integer[index.size()]), col);
		map.remove(key);

	}

	private static void writeMergedFile() throws IOException {

		BufferedReader br = new BufferedReader(new FileReader("E1.txt"));
		BufferedReader br1 = new BufferedReader(new FileReader("E2.txt"));
		// stream for writing merged tuples
		BufferedWriter bwm = new BufferedWriter(new FileWriter("merged.txt", true));
		DataReader dr = null;
		String s = br.readLine();
		String s1 = br1.readLine();

		while (s != null || s1 != null) {
			// System.out.println("s="+s+" s1="+s1);
			String str[];
			String str1[];
			int key1, key2;
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			if (s != null && s1 != null) {
				str = (s.split(" "));
				str1 = (s1.split(" "));
				key1 = Integer.parseInt(str[0]);
				key2 = Integer.parseInt(str1[0]);
				if (key1 == key2) {

					for (int i = 0; i < str[1].length(); i++) {
						int index = i + 1;
						if (str[1].charAt(i) == '1') {
							dr = new DataReader(fileAddress1);
							tuples.add(dr.readTupleAt(index));
						}
					}
					for (int i = 0; i < str1[1].length(); i++) {
						int index = i + 1;
						if (str1[1].charAt(i) == '1') {
							DataReader dr2 = new DataReader(fileAddress2);
							tuples.add(dr2.readTupleAt(index));
						}
					}

					int latestUpdate = 0;
					Tuple latestTuple = null;
					for (Tuple t : tuples) {
						if (latestUpdate < t.getLastUpdateAsNum()) {
							latestUpdate = t.getLastUpdateAsNum();
							latestTuple = t;
						}
					}
					bwm.write(latestTuple.toString() + "\r");
					dr.delink();
					s = br.readLine();
					s1 = br1.readLine();
				} else if (key1 < key2) {

					for (int i = 0; i < str[1].length(); i++) {
						int index = i + 1;
						if (str[1].charAt(i) == '1') {
							dr = new DataReader(fileAddress1);
							tuples.add(dr.readTupleAt(index));
						}
					}

					int latestUpdate = 0;
					Tuple latestTuple = null;
					for (Tuple t : tuples) {
						if (latestUpdate < t.getLastUpdateAsNum()) {
							latestUpdate = t.getLastUpdateAsNum();
							latestTuple = t;
						}
					}

					bwm.write(latestTuple.toString() + "\r");
					dr.delink();
					s = br.readLine();
				} else if (key1 > key2) {

					for (int i = 0; i < str1[1].length(); i++) {
						int index = i + 1;
						if (str1[1].charAt(i) == '1') {
							dr = new DataReader(fileAddress2);
							tuples.add(dr.readTupleAt(index));
						}
					}

					int latestUpdate = 0;
					Tuple latestTuple = null;
					for (Tuple t : tuples) {
						if (latestUpdate < t.getLastUpdateAsNum()) {
							latestUpdate = t.getLastUpdateAsNum();
							latestTuple = t;
						}
					}

					bwm.write(latestTuple.toString() + "\r");
					dr.delink();
					s1 = br1.readLine();
				}
			} else if (s == null && s1 != null) {

				str1 = (s1.split(" "));

				for (int i = 0; i < str1[1].length(); i++) {
					int index = i + 1;
					if (str1[1].charAt(i) == '1') {
						dr = new DataReader(fileAddress2);
						tuples.add(dr.readTupleAt(index));
					}
				}

				int latestUpdate = 0;
				Tuple latestTuple = null;
				for (Tuple t : tuples) {
					if (latestUpdate < t.getLastUpdateAsNum()) {
						latestUpdate = t.getLastUpdateAsNum();
						latestTuple = t;
					}
				}

				bwm.write(latestTuple.toString() + "\r");
				dr.delink();
				s1 = br1.readLine();
			} else if (s1 == null && s != null) {
				str = (s.split(" "));
				for (int i = 0; i < str[1].length(); i++) {
					int index = i + 1;
					if (str[1].charAt(i) == '1') {
						dr = new DataReader(fileAddress1);
						tuples.add(dr.readTupleAt(index));
					}
				}

				int latestUpdate = 0;
				Tuple latestTuple = null;
				for (Tuple t : tuples) {
					if (latestUpdate < t.getLastUpdateAsNum()) {
						latestUpdate = t.getLastUpdateAsNum();
						latestTuple = t;
					}
				}

				bwm.write(latestTuple.toString() + "\r");
				dr.delink();
				s = br.readLine();
			}

		}

		bwm.close();
		br.close();
		br1.close();
		// System.out.print(t.toString()+"\n");

	}

	private static void writeBitmapIndex(BufferedWriter bwu, BufferedWriter bwc, Integer key, Integer[] index,
			String col) throws IOException {
		String Key = "" + key;
		if (col.startsWith("E")) {
			Key = String.format("%08d", key);
		} else if (col.startsWith("D")) {
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
			String bin = Integer.toBinaryString(ind - 1);
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
