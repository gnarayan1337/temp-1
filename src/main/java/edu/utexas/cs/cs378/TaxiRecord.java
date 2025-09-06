package edu.utexas.cs.cs378;

public class TaxiRecord implements Comparable<TaxiRecord> {
    // All 17 fields from the CSV
    private String medallion;           // 0
    private String hackLicense;         // 1  
    private String pickupDatetime;      // 2
    private String dropoffDatetime;     // 3
    private String tripTimeInSecs;      // 4 (keep as String to handle invalid data)
    private String tripDistance;       // 5
    private String pickupLongitude;     // 6
    private String pickupLatitude;      // 7
    private String dropoffLongitude;    // 8
    private String dropoffLatitude;     // 9
    private String paymentType;         // 10
    private float fareAmount;           // 11 ← KEY FIELD for sorting
    private String surcharge;          // 12
    private String mtaTax;             // 13
    private String tipAmount;          // 14
    private String tollsAmount;        // 15
    private String totalAmount;        // 16
    
    // Store original line for easy reconstruction
    private String originalLine;
    
    // Constructor from CSV line
    public TaxiRecord(String csvLine) throws IllegalArgumentException {
        this.originalLine = csvLine;
        String[] fields = csvLine.split(",");
        
        if (fields.length != 17) {
            throw new IllegalArgumentException("Invalid CSV line - not 17 fields");
        }
        
        try {
            this.medallion = fields[0];
            this.hackLicense = fields[1];
            this.pickupDatetime = fields[2];
            this.dropoffDatetime = fields[3];
            this.tripTimeInSecs = fields[4];
            this.tripDistance = fields[5];
            this.pickupLongitude = fields[6];
            this.pickupLatitude = fields[7];
            this.dropoffLongitude = fields[8];
            this.dropoffLatitude = fields[9];
            this.paymentType = fields[10];
            
            // Parse fare amount - this is critical for sorting
            this.fareAmount = Float.parseFloat(fields[11].trim());
            
            this.surcharge = fields[12];
            this.mtaTax = fields[13];
            this.tipAmount = fields[14];
            this.tollsAmount = fields[15];
            this.totalAmount = fields[16];
            
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid fare amount in CSV line: " + fields[11]);
        }
    }
    
    // Getter for fare amount (needed for sorting)
    public float getFareAmount() {
        return fareAmount;
    }
    
    // Method to convert back to original CSV string
    public String toCsvString() {
        return originalLine;
    }
    
    // Implement Comparable interface for sorting by fare amount
    @Override
    public int compareTo(TaxiRecord other) {
        // Sort in ascending order (smallest to largest)
        return Float.compare(this.fareAmount, other.fareAmount);
    }
    
    // Override toString for debugging
    @Override
    public String toString() {
        return "TaxiRecord{fareAmount=" + fareAmount + ", original='" + originalLine + "'}";
    }
    
    // Override equals and hashCode for completeness
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TaxiRecord that = (TaxiRecord) obj;
        return originalLine.equals(that.originalLine);
    }
    
    @Override
    public int hashCode() {
        return originalLine.hashCode();
    }
}