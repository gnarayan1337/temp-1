package edu.utexas.cs.cs378;

class TaxiRecordWithSource implements Comparable<TaxiRecordWithSource> {
    TaxiRecord record;
    int fileIndex;  // Which chunk file this came from
    
    public TaxiRecordWithSource(TaxiRecord record, int fileIndex) {
        this.record = record;
        this.fileIndex = fileIndex;
    }
    
    @Override
    public int compareTo(TaxiRecordWithSource other) {
        return this.record.compareTo(other.record); // Compare by fare_amount
    }
}