package simpledb;
import java.util.*;


/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	private Type[] types;
	private String[] names;


    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert (typeAr != null);
                
    	types = typeAr;
    	if (fieldAr != null) {
    		names = fieldAr;
    	} else {
    		names = new String[typeAr.length];
    	}
    }


    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	types = typeAr;
    	names = new String[types.length];
    }


    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return types.length;
    }


    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	assert (names != null);
    	if (i > names.length) {
            throw new NoSuchElementException("Field " + i + " does not exist");
        }
        
    	return names[i];
    }
    
    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i > types.length) {
            throw new NoSuchElementException("Type " + i + " does not exist");
        }


        assert (i >= 0);
    	return types[i];
    }


    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if (name == null) {
            throw new NoSuchElementException("null is not a valid field name");
        }


        if (names == null) {
            throw new NoSuchElementException("No fields have names");
        }


    	assert (types.length == names.length);
    	for (int i = 0; i < names.length; i++) {
            String s = names[i];
            if (s == null) continue;
            if (s.equals(name)) return i;
    	}
    	
    	//assert (false);
    	throw new NoSuchElementException("Type does not have type: " + name);
    }


    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (Type t : types) {
    		size += t.getLen();
    	}
    	
        return size;
    }
    
    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        assert (td1 != null);
        assert (td2 != null);
        
        int firstSize = td1.numFields();
        int totalLength = td1.numFields() + td2.numFields();
        Type[] newType = new Type[totalLength];
        String[] newFields = new String[totalLength];


        for (int i = 0; i < totalLength; i++) {
            if (i < td1.numFields()) {
                newType[i] = td1.getFieldType(i);
                newFields[i] = td1.getFieldName(i);
            } else {
                newType[i] = td2.getFieldType(i - firstSize);
                newFields[i] = td2.getFieldName(i - firstSize);
            }
        }


        return new TupleDesc(newType, newFields);
    }


    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        try {
            TupleDesc other = (TupleDesc) o;
            if (other.getSize() != this.getSize()) return false;
            for (int i = 0; i < types.length; i++) {
                if (types[i] != other.types[i]) return false;
            }
            return true;
        } catch (ClassCastException classException) {
            // o wasn't a TupleDesc
            return false;
        } catch (NullPointerException nullPE) {
            // Received a null object
            return false;
        }
    }


    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }


    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < types.length; i++) {
            Type type = types[i];
            String name = "null";
            if (names != null) {
                name = names[i];
            }


            sb.append(type.toString() + "[" + i + "](" + name + "[" + i + "]),");
        }


        return sb.toString();
    }
}
