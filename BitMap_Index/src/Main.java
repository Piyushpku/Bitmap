import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.TreeMap;


/**
 * @author ekjot
 *
 */
public class Main {

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
	 * *sublist should contain key and its index in provided original file
	 * 2. 
	 * 
	 */

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		System.out.println("File 1:");
		fileAddress1 ="C:\\Users\\ekjot\\git\\BitMap_Index\\BitMap_Index\\Data\\Data1.txt"; //getFileAddress();

		System.out.println("\nFile 2:");
		fileAddress2 = "C:\\Users\\ekjot\\git\\BitMap_Index\\BitMap_Index\\Data\\Data2.txt";//getFileAddress();

		File f = new File("merged.txt");
		if (f.exists()) {
			f.delete();
		}
		/*
		 * C:\Users\ekjot\git\BitMap_Index\BitMap_Index\Data\Data.txt
		 */

		Double start = (double) System.nanoTime();
		Double time = start;

		System.out.println("\nConverting files to sublists.");
		byte numOfSublists = phaseOne(fileAddress1, fileAddress2);
		System.out.println("Sublists written: " + numOfSublists);
		System.out.println("Time taken: " + (System.nanoTime() - time) / 1000000000 + " sec");

		time = (double) System.nanoTime();

		System.out.println("\nWriting Uncompressed, Compressed and Merged files.");
		buildBitmapIndex(numOfSublists);
		System.out.println("Files written.");
		System.out.println("Time taken: " + (System.nanoTime() - time) / 1000000000 + " sec");

		writeOutput();

		System.out.println("\nTotal time : " + (System.nanoTime() - start) / 1000000000 + " sec");

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

	private static byte phaseOne(String fileAddress1, String fileAddress2) throws IOException {

		byte numOfSublists = 0;
		int numOfTuples = 0;
		boolean file1Complete = false, file2Complete = false;

		DataReader dr = new DataReader(fileAddress1);
		byte firstByte = dr.readByte();

		TreeMap<Integer, ArrayList<Integer>> empHash = new TreeMap<Integer, ArrayList<Integer>>();
		TreeMap<Integer, ArrayList<Integer>> genderHash = new TreeMap<Integer, ArrayList<Integer>>();
		TreeMap<Integer, ArrayList<Integer>> deptHash = new TreeMap<Integer, ArrayList<Integer>>();

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
			int lastUpdate = tuple.getLastUpdateAsNum();

			addToMap(empid, numOfTuples, empHash, true, lastUpdate);
			addToMap(gender, numOfTuples, genderHash, false, 0);
			addToMap(dept, numOfTuples, deptHash, false, 0);
			
			// read a small batch only and write it to disk; refresh treemaps
			if (numOfTuples % SUBLIST_SIZE == 0) {

				writeSublist(++numOfSublists, empHash, genderHash, deptHash);

				empHash = new TreeMap<Integer, ArrayList<Integer>>();
				genderHash = new TreeMap<Integer, ArrayList<Integer>>();
				deptHash = new TreeMap<Integer, ArrayList<Integer>>();
			}
			firstByte = dr.readByte();
		}

		if (!empHash.isEmpty()) {
			writeSublist(++numOfSublists, empHash, genderHash, deptHash);
		}
		TOTAL_TUPLES = numOfTuples;

		return numOfSublists;
	}

	private static void addToMap(int key, int index, TreeMap<Integer, ArrayList<Integer>> map, boolean isEmpid,
			int lastUpdate) {
		if (!map.containsKey(key)) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			list.add(index);

			if (isEmpid) {
				list.add(index);
				list.add(lastUpdate);
			}
			map.put(key, list);
		} // if treemap have given key, add index to its arraylist
		else {
			if (isEmpid) {
				int last = map.get(key).size() - 1;
				int oldLastUpdate = map.get(key).get(last);
				int oldIndex = map.get(key).get(last - 1);
				map.get(key).remove(last);
				map.get(key).remove(last - 1);
				map.get(key).add(index);
				if (lastUpdate > oldLastUpdate) {
					map.get(key).add(index);
					map.get(key).add(index);
				} else {
					map.get(key).add(oldIndex);
					map.get(key).add(oldLastUpdate);
				}
				return;
			}
			map.get(key).add(index);

		}

	}

	private static void writeSublist(byte numOfSublists, TreeMap<Integer, ArrayList<Integer>> empHash,
			TreeMap<Integer, ArrayList<Integer>> genderHash, TreeMap<Integer, ArrayList<Integer>> deptHash)
			throws IOException {

		BufferedWriter br = new BufferedWriter(new FileWriter("sublists\\" + numOfSublists + "_E.txt"));

		for (int empid : empHash.keySet()) {
			br.write(String.format("%08d", empid));
			for (int index : empHash.get(empid)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter("sublists\\" + numOfSublists + "_D.txt"));

		for (int dept : deptHash.keySet()) {
			br.write(String.format("%03d", dept));
			for (int index : deptHash.get(dept)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

		br = new BufferedWriter(new FileWriter("sublists\\" + numOfSublists + "_G.txt"));

		for (int gender : genderHash.keySet()) {
			br.write(gender + "");
			for (Integer index : genderHash.get(gender)) {
				br.write(" " + index);
			}
			br.write("\r\n");
		}

		br.close();

	}

	private static void buildBitmapIndex(byte numOfSublists) throws IOException {

		makeBitmapIndex(numOfSublists, 'E');
		makeBitmapIndex(numOfSublists, 'D');
		makeBitmapIndex(numOfSublists, 'G');

	}

	private static void makeBitmapIndex(byte numOfSublists, char col) throws IOException {
		// list to store all reading streams
		ArrayList<DataReader> DR = new ArrayList<DataReader>();

		// linking streams to all sublists
		for (Integer i = 1; i <= numOfSublists; i++) {
			DR.add(new DataReader("sublists\\" + i + "_" + col + ".txt"));
		}
		// DR.trimToSize();

		// Map to store K,V as key and a list of its indexes
		TreeMap<Integer, ArrayList<Integer>> map = new TreeMap<Integer, ArrayList<Integer>>();

		// stream for writing uncompressed indexes
		BufferedWriter bwu = new BufferedWriter(new FileWriter(col + ".txt"));

		// stream for writing compressed indexes
		BufferedWriter bwc = new BufferedWriter(new FileWriter(col + "_compressed.txt"));

		if (WRITE_UNCOMPRESSED) {
			//bwu.nullWriter();
		}
		if (WRITE_COMPRESSED) {
			//bwc.nullWriter();
		}

		// MERGING ALGORITHM
		// read first line of every sublist into a list
		//// if two sublists have same key, merge/append those in list
		// sort the list from min key to max(used TreeMap, so no need of sort)
		// write bitmap index of min key to a file
		// read next line from all sublists
		// repeat untill no sublist is left.

		// repeat untill all streams are done or in other words, DR is empty
		while (!DR.isEmpty()) {

			// fill list with a line from each stream
			fillMap(DR, map, col);

			if (map.isEmpty()) {
				break;
			}

			// write bitmap index of min key

			writeBIForMinKey(bwu, bwc, map, col);

		}

		// if there are some key left in list even after all streams are finished
		while (!map.isEmpty()) {
			writeBIForMinKey(bwu, bwc, map, col);
		}

		bwu.close();
		bwc.close();
	}

	private static void fillMap(ArrayList<DataReader> DR, TreeMap<Integer, ArrayList<Integer>> map, char col)
			throws IOException {
		// used iterator because of concurrent modification to DR list
		Iterator<DataReader> itr = DR.iterator();

		// traverse through all streams
		while (itr.hasNext()) {
			DataReader dr = itr.next();
			String str = dr.readLine();

			// if there is no next line, meaning stream/sublist is complete
			if (str == null) {
				itr.remove(); // remove it from DR list
				continue; // continue to next stream
			}

			// adding string containing key and indexes to list
			addStrToMap(str, map, col);

		}

	}

	private static void addStrToMap(String str, TreeMap<Integer, ArrayList<Integer>> map, char col) {
		// splitting the line into key and list of index
		String[] kv = (str).split(" ");
		int key = Integer.parseInt(kv[0]);

		// if map do not have given key, add it along with list of index
		if (!map.containsKey(key)) {
			ArrayList<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i < kv.length; i++) {
				list.add(Integer.parseInt(kv[i]));
			}

			map.put(key, list);
		} // if map have given key, append list of index to its arraylist
		else {
			if (col == 'E') {
				int last = map.get(key).size() - 1;
				int oldLastUpdate = map.get(key).get(last);
				int oldInd = map.get(key).get(last - 1);

				map.get(key).remove(last);
				map.get(key).remove(last - 1);

				int newLastUpdate = Integer.parseInt(kv[kv.length - 1]);

				if (oldLastUpdate > newLastUpdate) {
					kv[kv.length - 1] = oldLastUpdate + "";
					kv[kv.length - 2] = oldInd + "";
				}
			}
			for (int i = 1; i < kv.length; i++) {
				map.get(key).add(Integer.parseInt(kv[i]));
			}

		}

	}

	private static void writeBIForMinKey(BufferedWriter bwu, BufferedWriter bwc,
			TreeMap<Integer, ArrayList<Integer>> map, char col) throws IOException {
		int key = map.firstKey();

		ArrayList<Integer> index = map.get(key);

		if (col == 'E') {
			// this will remove lastupdate from index and write merged file
			useLatestUpdate(index);
		}

		Collections.sort(index);

		writeBitmapIndex(bwu, bwc, key, index.toArray(new Integer[index.size()]), col);
		map.remove(key);

	}

	private static void useLatestUpdate(ArrayList<Integer> index) throws IOException {
		int last = index.size() - 1;

		// int lastUpdate = index.get(last);
		int ind = index.get(last - 1);

		index.remove(last);
		index.remove(last - 1);

		if (WRITE_MERGED) {
			writeMergedFile(ind);
		}
	}

	private static void writeMergedFile(int latestIndex) throws IOException {
		// stream for writing merged tuples
		BufferedWriter bwm = new BufferedWriter(new FileWriter("merged.txt", true));
		DataReader dr;
		if (latestIndex <= tuplesInFile1) {
			dr = new DataReader(fileAddress1);
		} else {
			dr = new DataReader(fileAddress2);
			latestIndex -= tuplesInFile1;
		}

		Tuple t = dr.readTupleAt(latestIndex);
		bwm.write(t.toString() + "\r");
		dr.delink();
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
