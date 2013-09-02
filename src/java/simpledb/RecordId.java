package simpledb;


/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId {
    private PageId _pageId;
    private int _tupleIndex;


    /** Creates a new RecordId referring to the specified PageId and tuple number.
     * @param pid the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        _pageId = pid;
        _tupleIndex = tupleno;
    }


    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        return _tupleIndex;
    }


    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return _pageId;
    }
    
    /**
     * Two RecordId objects are considered equal if they represent the same tuple.
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        try {
            RecordId recordId = (RecordId) o;
            return (recordId._pageId.equals(this._pageId)) &&
                (recordId._tupleIndex == this._tupleIndex);
        } catch (Exception e) {
            return false;
        }
    }
    
    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // Hash: Take bottom 16 bits of page id and tuple index
        // Hash top 16 bits is page id, bottom 16 bits is tuple index
        int mask = 0x0000FFFF;
        int hash = (_pageId.pageNumber() & mask) << 16;
        return hash | (_tupleIndex & mask);
    }
}
