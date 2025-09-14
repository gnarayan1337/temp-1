package edu.utexas.cs.cs378;

public class TaxiRecord {
    // Only the 3 fields we need for MapReduce
    private String medallion;      // Taxi ID (for counting unique taxis)
    private String hackLicense;    // Driver ID (for grouping)
    private float totalAmount;     // Total earnings (for summing)
    
    // Keep track of validation status
    private boolean isValid;
    
    // Constructor from CSV line
    public TaxiRecord(String csvLine) {
        this.isValid = false;
        
        try {
            String[] fields = csvLine.split(",");
            
            // Check if we have exactly 17 fields (16 commas)
            if (fields.length != 17) {
                return; // Invalid - wrong number of fields
            }
            
            // Extract only the 3 fields we need
            this.medallion = fields[0].trim();        // Taxi ID
            this.hackLicense = fields[1].trim();      // Driver ID
            
            // Parse and validate total amount
            String totalAmountStr = fields[16].trim();
            this.totalAmount = Float.parseFloat(totalAmountStr);
            
            // Additional validation
            if (this.totalAmount <= 0 || this.totalAmount > 500.0f) {
                return; // Invalid - negative or too expensive
            }
            
            // Optional: Validate total amount calculation
            if (shouldValidateTotalSum()) {
                float fareAmount = Float.parseFloat(fields[11].trim());
                float surcharge = Float.parseFloat(fields[12].trim());
                float mtaTax = Float.parseFloat(fields[13].trim());
                float tipAmount = Float.parseFloat(fields[14].trim());
                float tollsAmount = Float.parseFloat(fields[15].trim());
                
                float expectedTotal = fareAmount + surcharge + mtaTax + tipAmount + tollsAmount;
                if (Math.abs(expectedTotal - this.totalAmount) > 0.01f) {
                    return; // Invalid - total doesn't match sum
                }
            }
            
            // If we get here, the record is valid
            this.isValid = true;
            
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
            // Invalid - parsing failed
            this.isValid = false;
        }
    }
    
    // Helper method to decide whether to validate total sum (expensive operation)
    private boolean shouldValidateTotalSum() {
        return true; // Set to false if performance is an issue
    }
    
    // Getters for the 3 key fields
    public String getMedallion() {
        return medallion;
    }
    
    public String getHackLicense() {
        return hackLicense;
    }
    
    public float getTotalAmount() {
        return totalAmount;
    }
    
    public boolean isValid() {
        return isValid;
    }
    
    // Override toString for debugging
    @Override
    public String toString() {
        return "TaxiRecord{medallion='" + medallion + "', hackLicense='" + hackLicense + 
               "', totalAmount=" + totalAmount + ", isValid=" + isValid + "}";
    }
}