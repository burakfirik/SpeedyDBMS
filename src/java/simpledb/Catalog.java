package simpledb;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;




/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */


public class Catalog {
    // Wrapper about Table information
    private class Table
    {
        private String name;
        private String primaryKey;
    
        public Table(String name, String primaryKey) {
            this.name = name;
            this.primaryKey = primaryKey;
        }


        public String getName() { return name; }
        public String getPrimaryKey() { return primaryKey; }
        public String toString() { return "Table " + this.name; }
    } // End Private Class Table




    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        files = new HashMap<Table, DbFile>();
        tableMap = new HashMap<Integer, Table>();
    }


    private HashMap<Table, DbFile> files;
    private HashMap<Integer, Table> tableMap;


    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        assert (name != null);
        assert (file != null);


        Table newTable = new Table(name, pkeyField);
        this.files.put(newTable, file);
        this.tableMap.put(file.getId(), newTable);
    }


    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }


    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }


    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        for (Table t : this.files.keySet()) {
            if (t.getName().equals(name)) return this.files.get(t).getId();
        }


        throw new NoSuchElementException("No table: " + name + "exists");
    }


    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        return getDatabaseFile(tableid).getTupleDesc();
    }


    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        Table table = getTable(tableid);
        if (!this.files.containsKey(table)) {
            throw new NoSuchElementException("No file from table: " + table);
        }
        
        return this.files.get(table);
    }


    private Table getTable(int tableid) throws NoSuchElementException {
    	if (!this.tableMap.containsKey(tableid)) {
            throw new NoSuchElementException("No table with id: " + tableid);
        }


        return this.tableMap.get(tableid);
    }


    public String getPrimaryKey(int tableid) {
        return getTable(tableid).getPrimaryKey();
    }


    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return null;
    }


    public String getTableName(int id) {
        return getTable(id).getName();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        this.tableMap.clear();
        this.files.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));


            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
