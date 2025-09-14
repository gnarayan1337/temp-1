
package edu.utexas.cs.cs378;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

public class MainClient {

	static public int portNumber = 33333;
	static public String hostName = "localhost";

	static public int batchSize = 4000;
	private static Socket mySocket;

	/**
	 * A main method to run examples.
	 *
	 * @param args not used
	 */
	public static void main(String[] args) {

		if (args.length >= 3) {
			batchSize = Integer.parseInt(args[0]);
			hostName = args[1];
			portNumber = Integer.parseInt(args[2]);
			System.out.println("Using args: batchSize=" + batchSize + ", hostname=" + hostName + ", port=" + portNumber);
		} else {
			System.out.println("Using defaults: batchSize=" + batchSize + ", hostname=" + hostName + ", port=" + portNumber);
		}

		try {
			System.out.println("Attempting to connect to " + hostName + ":" + portNumber);
			mySocket = new Socket(hostName, portNumber);
			System.out.println("Successfully connected to server!");

			DataInputStream dis = new DataInputStream(mySocket.getInputStream());
			DataOutputStream dos = new DataOutputStream(mySocket.getOutputStream());
			System.out.println("Data streams established");

			// This is a demo data
			// 1. Clean your data
			// 2. Pre-process your data, map it for example to other forms
			// 3. Send it to the server like the following.

			// Real CSV processing
			String csvFilePath = "/Users/gnarayan/Desktop/cloud_computing/cs378-cloud-computing-a-21-gnarayan1337/taxi-data-sorted-large-n.csv.csv";
			int batchSize = 100_000;
			
			System.out.println("Starting CSV processing...");
			List<DataItem> dataItems;
			try {
				dataItems = Utils.processCSVInBatches(csvFilePath, batchSize);
				System.out.println("CSV processing complete. Found " + dataItems.size() + " unique drivers.");
			} catch (Exception e) {
				System.err.println("Error processing CSV file: " + e.getMessage());
				e.printStackTrace();
				return;
			}

			// continue with exiting paging and sending code
			List<byte[]> pages = Utils.packageToPages(dataItems);

			// Then we send the pages over to the server.
			for (byte[] bs : pages) {

				// tell the server that we have data to send
				dos.writeInt(1);
				dos.flush();

				System.out.println("Sending a page of data to server");
				// then write the entire page and flush it
				dos.write(bs);
				dos.flush();

				// does not arrive on the other side if we do not write and flush it

				while (dis.readInt() != 1) {
					System.out.println("While true");
					// Here client asks the server if the it can process more data.
					//
					try {
						// !TODO: We sleep here but you can do a lot more thing.s

						Thread.sleep(500);
						System.out.println("Waiting for the server ... ");
					} catch (InterruptedException e) {

						e.printStackTrace();
					}
				}
			}

			// Good bye! We have no more data.
			// Tell the server to terminate.
			dos.writeInt(0);
			dos.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}