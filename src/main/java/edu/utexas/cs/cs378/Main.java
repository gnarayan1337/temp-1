package edu.utexas.cs.cs378;

import java.util.*;
import java.io.*;

public class Main {

	/**
	 * A main method to run examples.
	 *
	 * @param args not used
	 */
	public static void main(String[] args) {

		// pass the file name as the first argument. 
		// We can also accept a .bz2 file
		
		// This line is just for Kia :) 
		// You should pass the file name and path as first argument of this main method. 
		String file = "/Users/gnarayan/Desktop/cloud_computing/cs378-cloud-computing-a-01-gnarayan1337/taxi-data-sorted-small.csv";
		
		if(args.length>0)
			file=args[0];

		int batchSize = 1_000_000;

		int chunkNumber = externalSort(file, batchSize);

		ArrayList<String> errorLines = new ArrayList<>();
		errorLines = performDataCleanup(file);

		writeErrorLinesToFile(errorLines, "errorlines");	

		mergeChunks(chunkNumber);
	}

	public static int externalSort(String filename, int batchSize) {
		int chunkNumber = 0;

		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;

			List<TaxiRecord> batch = new ArrayList<>();
			int totalLinesProcessed = 0;

			while((line = br.readLine()) != null) {
				totalLinesProcessed++;

				try {
					// parse each line into a taxRecord object
					TaxiRecord record = new TaxiRecord(line); // constructor handles logic

					// store records in RAM
					batch.add(record);

					// check if batch is full
					if (batch.size() >= batchSize) {
						chunkNumber++;
						
						// TODO: sort the batch
						Collections.sort(batch);
						// Write sorted chunk to file
						String chunkFilename = "chunk_" + String.format("%03d", chunkNumber) + ".txt";

						try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFilename))) {
							for (TaxiRecord rec : batch) {
								writer.write(rec.toCsvString());
								writer.newLine();
							}
							System.out.println("Written chunk to: " + chunkFilename);
						} catch (IOException e) {
							System.err.println("Error writing chunk file: " + e.getMessage());
						}	

						// clear batch out of ram
						batch.clear();
					
					}
				} catch (IllegalArgumentException e) {
					// skip invalid lines
					continue;
				}
			}	

			if (!batch.isEmpty()) {
				chunkNumber++;

				System.out.println("Processing final batch " + chunkNumber + " with " + batch.size() + " records...");
				
				// Sort final batch
				Collections.sort(batch);
				
				// Write final chunk to file
				String chunkFilename = "chunk_" + String.format("%03d", chunkNumber) + ".txt";
				
				try (BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFilename))) {
					for (TaxiRecord record : batch) {
						writer.write(record.toCsvString());
						writer.newLine();
					}
					System.out.println("Written final chunk to: " + chunkFilename);
				} catch (IOException e) {
					System.err.println("Error writing final chunk file: " + e.getMessage());
				}
			}

			br.close();

		} catch (IOException e) {
			System.err.println("Error reading file: " + e.getMessage());
			e.printStackTrace();
		}

		return chunkNumber;
	}

	public static ArrayList<String> performDataCleanup(String filename) {
		ArrayList<String> errorLines = new ArrayList<>();
		int lineCount = 0;

		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line;

			while ((line = br.readLine()) != null) {
				// process line
				if (isValidLine(line)) {
					lineCount++;
				} else if (errorLines.size() < 5) {
					errorLines.add(line);
				}
			}
			br.close();
		} catch (IOException e) {
			System.err.println("Error reading file: " + e.getMessage());
			e.printStackTrace();
		}

		return errorLines;
	}

	public static void mergeChunks(int numberOfChunks) {
		try {
			// create priority queue for merge
			PriorityQueue<TaxiRecordWithSource> pq = new PriorityQueue<>();

			// array to hold all chunk file readers
			BufferedReader[] readers = new BufferedReader[numberOfChunks];

			// open all chunk files
			for (int i = 0; i < numberOfChunks; i++) {
				String filename = "chunk_" + String.format("%03d", i + 1) + ".txt";
				readers[i] = new BufferedReader(new FileReader(filename));
			}

			// read first record from each file and add to priorty queue
			for (int i = 0; i < readers.length; i++) {
				String line = readers[i].readLine();
				if (line != null) {
					try {
						TaxiRecord record = new TaxiRecord(line);
						pq.offer(new TaxiRecordWithSource(record, i)); // i specifies what file for the constructor
					} catch (IllegalArgumentException e) {
						continue;
					}
				}
			}

			// create output file
			BufferedWriter writer = new BufferedWriter(new FileWriter("SORTED-FILE-RESULT.txt"));
			long totalRecordsWritten = 0;

			// main merge loop
			while (!pq.isEmpty()) {
				// get the smallest record
				TaxiRecordWithSource smallest = pq.poll();

				// write to results
				writer.write(smallest.record.toCsvString());
				writer.newLine();
				totalRecordsWritten++;

				// read next record from the same file
				int fileIndex = smallest.fileIndex;
				String nextLine = readers[fileIndex].readLine();

				if (nextLine != null) {
					try {
						TaxiRecord nextRecord = new TaxiRecord(nextLine);
						pq.offer(new TaxiRecordWithSource(nextRecord, fileIndex));
					} catch (IllegalArgumentException e) {
						continue;
					}
				}
			}

			writer.close();

			for (BufferedReader reader : readers) {
				if (reader != null) {
					reader.close();
				}
			}

		} catch (IOException e) {
			System.err.println("Error during merge: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public static boolean isValidLine(String line) {
		// check if each line includes 17 commas
		String[] fields = line.split(",");
		if (fields.length != 17) {
			return false;
		}

		// check if the value with fare_amount can be converted to a float
		try {
			String fareAmountString = fields[11].trim();
			Float.parseFloat(fareAmountString);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}

	}

	public static void writeErrorLinesToFile(ArrayList<String> errorLines, String filename) {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
			if (errorLines.isEmpty()) {
				writer.write("No error lines found - all data is correctly formatted.");
				writer.newLine();
			} else {
				writer.write("Error lines found (showing up to 5):");
				writer.newLine();
				writer.newLine();
				
				for (int i = 0; i < Math.min(5, errorLines.size()); i++) {
					writer.write("Error " + (i + 1) + ": " + errorLines.get(i));
					writer.newLine();
				}
			}
			System.out.println("Error lines written to: " + filename);
		} catch (IOException e) {
			System.err.println("Error writing to file: " + e.getMessage());
		}
	}
	
}