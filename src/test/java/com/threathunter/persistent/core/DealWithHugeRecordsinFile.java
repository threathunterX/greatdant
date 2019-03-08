package com.threathunter.persistent.core;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import junit.framework.TestCase;

/**
 * @author Naresh Bhabat
 * 
Following  implementation helps to deal with extra large files in java.
This program is tested for dealing with 2GB input file.
There are some points where extra logic can be added in future.


Pleasenote: if we want to deal with binary input file, then instead of reading line,we need to read bytes from read file object.



It uses random access file,which is almost like streaming API.


 * ****************************************
Notes regarding executor framework and its readings.
Please note :ExecutorService executor = Executors.newFixedThreadPool(10);

 *  	   for 10 threads:Total time required for reading and writing the text in
 *         :seconds 349.317
 * 
 *         For 100:Total time required for reading the text and writing   : seconds 464.042
 * 
 *         For 1000 : Total time required for reading and writing text :466.538 
 *         For 10000  Total time required for reading and writing in seconds 479.701
 *
 * 
 */
public class DealWithHugeRecordsinFile extends TestCase {

	static final String FILEPATH = "C:\\springbatch\\bigfile1.txt.txt";
	static final String FILEPATH_WRITE = "C:\\springbatch\\writinghere.txt";
	static volatile RandomAccessFile fileToWrite;
	static volatile RandomAccessFile file;
	static volatile String fileContentsIter;
	static volatile int position = 0;

	public static void main(String[] args) throws IOException, InterruptedException {
		long currentTimeMillis = System.currentTimeMillis();

		try {
			fileToWrite = new RandomAccessFile(FILEPATH_WRITE, "rw");//for random write,independent of thread obstacles 
			file = new RandomAccessFile(FILEPATH, "r");//for random read,independent of thread obstacles 
			seriouslyReadProcessAndWriteAsynch();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Thread currentThread = Thread.currentThread();
		System.out.println(currentThread.getName());
		long currentTimeMillis2 = System.currentTimeMillis();
		double time_seconds = (currentTimeMillis2 - currentTimeMillis) / 1000.0;
		System.out.println("Total time required for reading the text in seconds " + time_seconds);

	}

	/**
	 * @throws IOException
	 * Something  asynchronously serious
	 */
	public static void seriouslyReadProcessAndWriteAsynch() throws IOException {
		ExecutorService executor = Executors.newFixedThreadPool(10);//pls see for explanation in comments section of the class
		while (true) {
			String readLine = file.readLine();
			if (readLine == null) {
				break;
			}
			Runnable genuineWorker = new Runnable() {
				@Override
				public void run() {
					// do hard processing here in this thread,i have consumed
					// some time and eat some exception in write method.
					writeToFile(FILEPATH_WRITE, readLine);
					// System.out.println(" :" +
					// Thread.currentThread().getName());

				}
			};
			executor.execute(genuineWorker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		System.out.println("Finished all threads");
		file.close();
		fileToWrite.close();
	}

	/**
	 * @param filePath
	 * @param data
	 * @param position
	 */
	private static void writeToFile(String filePath, String data) {
		try {
			// fileToWrite.seek(position);
			data = "\n" + data;
			if (!data.contains("Randomization")) {
				return;
			}
			System.out.println("Let us do something time consuming to make this thread busy"+(position++) + "   :" + data);
			System.out.println("Lets consume through this loop");
			int i=1000;
			while(i>0){
			
				i--;
			}
			fileToWrite.write(data.getBytes());
			throw new Exception();
		} catch (Exception exception) {
			System.out.println("exception was thrown but still we are able to proceeed further"
					+ " \n This can be used for marking failure of the records");
			//exception.printStackTrace();

		}

	}
}