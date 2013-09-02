package simpledb;


import java.util.*;


public class HeapPageIterator implements Iterator<Tuple> {
    private HeapPage page;
    private int numTuples;
    private int currentTuple;
        
    // Assumes pages cannot be modified while iterating over them
    // Iterates over only valid tuples
    public HeapPageIterator(HeapPage page) {
        this.page = page;
        this.currentTuple = 0;
        this.numTuples = this.page.getNumValidTuples();
    }
        
    public boolean hasNext() {
        return currentTuple < numTuples;
    }
        
    public Tuple next() {
        return this.page.tuples[currentTuple++];
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot remove on HeapPageIterator");
    }
}
