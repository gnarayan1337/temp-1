package edu.utexas.cs.cs378;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.ArrayList;

public class MainServer {

	static public int portNumber = 33333;

	/**
	 * A main method to run examples.
	 *
	 * @param args not used
	 */
	public static void main(String[] args) {


		if (args.length >= 1) {
			portNumber = Integer.parseInt(args[0]);
			System.out.println("Using port from args: " + portNumber);
		} else {
			System.out.println("Using default port: " + portNumber);
		}

		ServerSocket serverSocket;
		Socket clientSocket;

		try {
			serverSocket = new ServerSocket(portNumber);
			System.out.println("Server is running on port number " + portNumber);

			System.out.println("Waiting for client connection ... ");

			clientSocket = serverSocket.accept();

			// after accept the data input stream is established and we have a channel between 
			// the two machines. the channel is two sided meaning both can write and read
			// each time the client sends something it can get something back.

			DataInputStream dis = new DataInputStream(clientSocket.getInputStream());

			// Use the output stream if you need it.
			DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
			System.out.println("Server is hearing on port " + portNumber);
			int hasData; 
			
			// Global driver aggregation for REDUCE phase
		Map<String, DriverAggregation> globalDriverMap = new HashMap<>();
		//---- stuff above and bleow one line abo e one line bloew
		// Priority queue to keep track of top 10 drivers (min-heap, so smallest earnings at top)
		PriorityQueue<DriverAggregation> top10Queue = new PriorityQueue<>(10, 
			(a, b) -> Float.compare(a.getTotalEarnings(), b.getTotalEarnings()));
			
			// how we send pages over
			while(true) {
				// first send an integer
				// integer is just a flag
				// if 1 we have data from teh client size, if 0 then last one?
				hasData = dis.readInt();
				
				System.out.println("Flag is:" + hasData);
				
				if(hasData==1) {
				System.out.println("We have a page of data to process!");
				
				// 64 MB page Size
				byte[] page = new byte[Const.PAGESIZE];

				// read the main byte array into memory
				dis.readFully(page);

				//TODO: Remove the Sleep when you run your program. This is just for demo. 
				// Thread.sleep(500); // not necessary REMOVE
				
				// now i read from the page 
				List<DataItem> dataItems = Utils.readFromAPage(page) ;
				
//---------
				// REDUCE PHASE: Process each DataItem (pre-aggregated driver data from client)
				for (DataItem dataItem : dataItems) {
					String driverID = dataItem.getLine();           // Driver ID
					float earnings = dataItem.getValueA();          // Total earnings from client
					int taxiCount = (int) dataItem.getValueB();     // Unique taxi count from client
					
					// Merge into global map (same driver might come from multiple client batches)
					DriverAggregation existing = globalDriverMap.get(driverID);
					if (existing == null) {
						globalDriverMap.put(driverID, new DriverAggregation(driverID, earnings, taxiCount));
					} else {
						existing.addEarnings(earnings);
						existing.addTaxiCount(taxiCount);
					}
				}
				
				System.out.println("Number of Objects received: " + dataItems.size() + 
								  ", Total unique drivers so far: " + globalDriverMap.size());
			//-----	
				
				// This tells the client to send more data. 
				// Give me more data if you have
				dos.writeInt(1);
				dos.flush();
				
				}else {
					System.out.println("Terminate because Flag is: " + hasData);
					break; // break out of while true if we get no more data. 					
				}
				
				
			}// End of while true
			//---------
			// ALL DATA RECEIVED - Now find top 10 drivers using priority queue
			System.out.println("\n=== PROCESSING COMPLETE ===");
			System.out.println("Total unique drivers processed: " + globalDriverMap.size());
			
			// Build top 10 using priority queue (simple and memory efficient)
			for (DriverAggregation driver : globalDriverMap.values()) {
				if (top10Queue.size() < 10) {
					// Queue not full, just add
					top10Queue.offer(driver);
				} else if (driver.getTotalEarnings() > top10Queue.peek().getTotalEarnings()) {
					// This driver has higher earnings than the lowest in our top 10
					top10Queue.poll();  // Remove lowest
					top10Queue.offer(driver);  // Add this driver
				}
			}
			
			// Convert queue to list and reverse for descending order
			List<DriverAggregation> top10List = new ArrayList<>(top10Queue);
			top10List.sort((a, b) -> Float.compare(b.getTotalEarnings(), a.getTotalEarnings()));
			
			// Output results in required format: (driverID, numTaxis, totalEarnings)
			System.out.println("\n=== TOP 10 DRIVERS WITH HIGHEST EARNINGS ===");
			for (int i = 0; i < top10List.size(); i++) {
				DriverAggregation driver = top10List.get(i);
				System.out.println((i + 1) + ". (" + driver.getDriverID() + ", " + 
								  driver.getTaxiCount() + ", " + 
								  String.format("%.2f", driver.getTotalEarnings()) + ")");
			}
			//---------




		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	//------- below
	// Simple helper class for driver aggregation in REDUCE phase
	private static class DriverAggregation {
		private String driverID;
		private float totalEarnings;
		private int taxiCount;
		
		public DriverAggregation(String driverID, float earnings, int taxis) {
			this.driverID = driverID;
			this.totalEarnings = earnings;
			this.taxiCount = taxis;
		}
		
		public void addEarnings(float earnings) {
			this.totalEarnings += earnings;
		}
		
		public void addTaxiCount(int count) {
			this.taxiCount += count;
		}
		
		// Getters
		public String getDriverID() { return driverID; }
		public float getTotalEarnings() { return totalEarnings; }
		public int getTaxiCount() { return taxiCount; }
	}

}

