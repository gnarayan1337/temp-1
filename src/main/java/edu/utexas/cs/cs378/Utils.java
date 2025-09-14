package edu.utexas.cs.cs378;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Utils {

	/**
	 * Reads back a list of DataItems from a byte array.
	 * 
	 * @param dataRead
	 * @return
	 */
	public static List<DataItem> readFromAPage(byte[] dataRead) {

		ByteBuffer byteBuffer = ByteBuffer.wrap(dataRead);

		// Get the number of objects that we have to read.
		int numberOfObjects = byteBuffer.getInt(); // 4 bytes
		
		System.out.println("number of Objects is: " + numberOfObjects );

		// pre-allocate a list of objects.
		List<DataItem> myDataItems = new ArrayList<DataItem>(numberOfObjects);

		// 1. read first the object size as an integer
		// 2. read back the object bytes
		// 3. de-serialze an object
		// 4. add the object to the list and return. 

		for (int i = 0; i < numberOfObjects; i++) {

			int objectSize = byteBuffer.getInt(); // 4 bytes
			byte[] objectBytes = new byte[objectSize]; // get the object bytes
			// now get the bytes of the object. 
			byteBuffer.get(objectBytes, 0, objectSize);

			// De-serialize an object from the objectButes
			DataItem myDataItem = new DataItem().deserializeFromBytes(objectBytes);
			
			// Add it to the return list.
			myDataItems.add(myDataItem);

		}

		return myDataItems;

	}

	/**
	 * This method packages a list of DataItems into list of pages. It may happen
	 * that some portion of a page at the end will remain empty.
	 * 
	 * @param dataItems
	 * @return
	 */
	public static List<byte[]> packageToPages(List<DataItem> dataItems) {

		int sizeCounter = 4; // we need 4 bytes of an int to write the number of objects at the beginning of
							 // each page.

		// initializations 
		List<byte[]> objectsInBytesTemp = new ArrayList<byte[]>();

		List<byte[]> pageList = new ArrayList<byte[]>();
	
		ByteBuffer byteBuffer = ByteBuffer.allocate(Const.PAGESIZE);

		// counter for objects in each page.
		int objectCounter = 0;
		
		
		for (int i = 0; i < dataItems.size(); i++) {
			
			// get the objects bytes and de-serialize them 
			DataItem myDataItem = dataItems.get(i);

			byte[] bytesOfIt = myDataItem.handSerializationWithByteBuffer();

			objectsInBytesTemp.add(bytesOfIt);

			// increase the size
			sizeCounter = sizeCounter + 4 + bytesOfIt.length;
			
			objectCounter++;

			// if we have enough objects for a page, then dump it to a page and get a new
			// page.
			if (sizeCounter > Const.PAGESIZE) {
                //skip the last object because it cause a page overflow. 
				//1. add the count of objects on this page. 
				byteBuffer.putInt(objectCounter-1);
				
				// We add all object, and skip the last object because it cause a page overflow. 
				for (int j = 0; j < objectsInBytesTemp.size() - 1 ; j++) {
					
					byteBuffer.putInt(objectsInBytesTemp.get(j).length);
					byteBuffer.put(objectsInBytesTemp.get(j));
				}
	
				
				// a page is ready
				pageList.add(byteBuffer.array());
				
				// reset for a new page and adding the last object before full. 
				byteBuffer = ByteBuffer.allocate(Const.PAGESIZE);
				byte[] lastObjectBeforeFull = objectsInBytesTemp.get(objectsInBytesTemp.size() -1 );
				
				// get a fresh list 
				objectsInBytesTemp = new ArrayList<byte[]>();
				// add the last object to the new page. 
				objectsInBytesTemp.add(lastObjectBeforeFull);
				
				sizeCounter = 4; // 4 for the object numbers, and 4 for the length of the last object. 
				sizeCounter = sizeCounter + 4 + lastObjectBeforeFull.length; 
				objectCounter = 1;
				

			}// end of IF statement. 

		}
		
		
		// one page at the end, for the case that we have data less than a page. 
		
		byteBuffer.putInt(objectCounter);
		for (int j = 0; j < objectsInBytesTemp.size(); j++) {
			
			byteBuffer.putInt(objectsInBytesTemp.get(j).length);
			byteBuffer.put(objectsInBytesTemp.get(j));
		}
		pageList.add(byteBuffer.array());
		System.out.println("Page number " + pageList.size() );

		System.out.println("Number of data pages to send to server is: " + pageList.size());

		return pageList;

	}

	
	/*
	 * Generates some example Data.
	 */
	public static List<DataItem> generateExampleData(int number) {
		List<DataItem> items = new ArrayList<DataItem>(number);

		for (int i = 0; i < number; i++) {
			DataItem myObject = new DataItem("ID-NUMBER-" + i, i + 1, i + 2);
			items.add(myObject);
		}
		return items;

	}

	public static List<DataItem> processCSVInBatches(String csvFilePath, int batchSize) {
		Map<String, DriverData> globalDriverMap = new HashMap<>(); 
		List<String> errorLines = new ArrayList<>();
		int totalErrorCount = 0;
		int totalLinesProcessed = 0;

		try (BufferedReader reader = Files.newBufferedReader(Paths.get(csvFilePath))) {
			String line;

			// process the file in batches to manage memory
			while ((line = reader.readLine()) != null) {
				// initialize batch variables
				Map<String, DriverData> batchDriverMap = new HashMap<>(); // temp storage for this batch
				int batchLineCount = 0;

				// process one batch of lines

				do {
					try {
						// prase and validate each csv line
						TaxiRecord trip = new TaxiRecord(line);

						if (trip.isValid()) {
							// extract 3 key pieces of data we need
							String driverId = trip.getHackLicense(); // driver identifier
							String taxiID = trip.getMedallion(); // taxi id
							float earnings = trip.getTotalAmount(); //money earned this trip

							// aggregate data in the batch hashmap
							DriverData driverData = batchDriverMap.get(driverId);
							if (driverData == null) {
								driverData = new DriverData(driverId);
								batchDriverMap.put(driverId, driverData);
							}

							//add this trip's data to driver's totals
							driverData.addEarnings(earnings);
							driverData.addTaxi(taxiID);
						} else {
							// handle invalid lines
							if (totalErrorCount < 5) {
								errorLines.add(line);
							}
							totalErrorCount++;
						}
					} catch (Exception e) {
						// handle parsing exceptions
						if (totalErrorCount < 5) {
							errorLines.add(line);
						}
						totalErrorCount++;
					}

					batchLineCount++;
					totalLinesProcessed++;

					// read next line for batch
					line = reader.readLine();
				} while (line != null && batchLineCount < batchSize);

				// merge this batch's data into the global driver map
				for (Map.Entry<String, DriverData> entry : batchDriverMap.entrySet()) {
					String driverID = entry.getKey();
					DriverData batchData = entry.getValue();

					DriverData globalData = globalDriverMap.get(driverID);
					if (globalData == null) {

						// first time seeing this driver globally
						globalDriverMap.put(driverID, batchData);
					} else {
						// merge with existing global data
						globalData.addEarnings(batchData.getTotalEarnings());
						for (String taxiID: batchData.getUniqueTaxiIDs()) {
							globalData.addTaxi(taxiID);
						}
					}
				}

				// clear batch memory
				batchDriverMap.clear();
				System.gc();

				// Progress update
				if (totalLinesProcessed % 500000 == 0) {
					System.out.println("Processed " + totalLinesProcessed + " lines, " + globalDriverMap.size() + " drivers");
				}

				if (line == null) {
					break;
				}
			}
		} catch (IOException e) {
			System.err.println("Error reading CSV file: " + e.getMessage());
			e.printStackTrace();
		}

		// Write error lines to file
		if (!errorLines.isEmpty()) {
			try (PrintWriter writer = new PrintWriter("errorLines.txt")) {
				for (String errorLine : errorLines) {
					writer.println(errorLine);
				}
				System.out.println("Wrote " + errorLines.size() + " error lines to errorLines.txt");
			} catch (IOException e) {
				System.err.println("Failed to write error file: " + e.getMessage());
			}
		}

		System.out.println("Processing complete: " + totalLinesProcessed + " lines, " + globalDriverMap.size() + " drivers");

		List<DataItem> dataItems = new ArrayList<>();
		for (DriverData driver : globalDriverMap.values()) {
			DataItem item = new DataItem(
				driver.getDriverID(),
				driver.getTotalEarnings(),
				driver.getUniqueTaxiCount()
			);
			dataItems.add(item);
		}

		return dataItems;

	}

	// helper class to store driver aggregation data
	private static class DriverData {
		private String driverID;
		private float totalEarnings;
		private Set<String> uniqueTaxiIDs;

		public DriverData(String driverID) {
			this.driverID = driverID;
			this.totalEarnings = 0.0f;
			this.uniqueTaxiIDs = new HashSet<>();
		}

		// add earnings from one trip
		public void addEarnings(float amount) {
			this.totalEarnings += amount;
		}

		// add a taxi id to the set
		public void addTaxi(String taxiID) {
			this.uniqueTaxiIDs.add(taxiID);
		}

		// getters
		public String getDriverID() { 
			return driverID; 
		}
		
		public float getTotalEarnings() { 
			return totalEarnings; 
		}
		
		public int getUniqueTaxiCount() { 
			return uniqueTaxiIDs.size(); 
		}
		
		public Set<String> getUniqueTaxiIDs() {
			return uniqueTaxiIDs;
		}
		
	}

}
