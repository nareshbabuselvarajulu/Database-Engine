//package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by shruti on 11/4/17.
 */
class DataTable{

    HashMap<String,List<ArrayList<PrimitiveValue>>> allTableData;
    HashMap<String,HashMap<String,HashMap<String,DBschema>>> schemaList;
    HashMap<String,MyIterator> myIter;
    MyIterator fileIterator;
    HashMap<Integer,String> tableNames;

    public DataTable(){
        allTableData=new HashMap<String,List<ArrayList<PrimitiveValue>>>();
        schemaList = new HashMap<String,HashMap<String,HashMap<String,DBschema>>>();
        myIter=new HashMap<String,MyIterator>();
        tableNames=new HashMap<Integer,String>();
        fileIterator=null;
    }

    public MyIterator getFileIterator() {
        return fileIterator;
    }

    public void setFileIterator(MyIterator fileIterator) {
        this.fileIterator = fileIterator;
    }

    public HashMap<String, List<ArrayList<PrimitiveValue>>> getAllTableData() {
        return allTableData;
    }

    public void setAllTableData(HashMap<String, List<ArrayList<PrimitiveValue>>> allTableData) {
        this.allTableData = allTableData;
    }

    public HashMap<String, HashMap<String, HashMap<String, DBschema>>> getSchemaList() {
        return schemaList;
    }

    public void setSchemaList(HashMap<String, HashMap<String, HashMap<String, DBschema>>> schemaList) {
        this.schemaList = schemaList;
    }

    public HashMap<String, MyIterator> getMyIter() {
        return myIter;
    }

    public void setMyIter(HashMap<String, MyIterator> myIter) {
        this.myIter = myIter;
    }

    public HashMap<Integer,String> getTableNames() {
        return tableNames;
    }

    public void setTableNames(HashMap<Integer,String> tableNames) {
        this.tableNames = tableNames;
    }

}