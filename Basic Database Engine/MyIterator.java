//package dubstep;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Limit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;


class MyIterator implements Iterator{

    ArrayList<PrimitiveValue> i_s=null;
    ArrayList<ArrayList<PrimitiveValue>> rows;
    HashMap<Integer,ArrayList<PrimitiveValue>> data;
    ArrayList<Integer> indices;
    BufferedReader dataIterator = null;
    String file;
    Expression where;
    List<Expression> whereList;
    BufferedReader br = null;
    BufferedReader br_hash = null;
    BufferedReader bufferedReader1 = null;
    BufferedReader bufferedReader2 = null;
    String joinColumn;
    List<String > columns;
    int rowIndex=0;
    long rowCount;
    Limit limit;
    Iterator<ArrayList<PrimitiveValue>> rowsIterator;
    HashMap<String,DBschema> schema;
    /*** hash index in mem ***/
    ArrayList<HashMap<Integer,ArrayList<PrimitiveValue>>> joinData;
    ArrayList<HashMap<PrimitiveValue,ArrayList>> hash_index;
    ArrayList<Integer> joinColumnsIndex;
    ArrayList<Integer> joinRemovalIndex;
    ArrayList<Integer[]> joinAdditionalIndex;
    HashMap<String, DBschema> joinSchema;
    ArrayList<ArrayList<PrimitiveValue>> joinedRecords = new ArrayList();
    int counterForNext = 0;
    ArrayList<BufferedReader> bufferedReaderArrayList = new ArrayList<>();
    ArrayList<ArrayList<PrimitiveValue>> joinedRecordsOnDisk;
    ArrayList<HashMap<Integer,ArrayList<PrimitiveValue>>> joinDataOnDisk;

    HashMap<Integer,HashMap<String,DBschema>> schemas;
    ArrayList<Integer> joinColumnsIndexOnDisk;
    ArrayList<String> columnNames;
    ArrayList<List<String>> columns3;
    int countReturnRecordsOnDisk = 0;

    /***btree in mem***/
    public MyIterator(String file,List<String> columns, HashMap<String,DBschema> schema, Limit limit,Expression where) throws IOException {
        this.file =file;
        this.schema = schema;
        this.rowCount = 0;
        this.limit = limit;
        dataIterator = new BufferedReader(new FileReader("/home/shruti/DB/Checkpoint2_on_disk/data/"+file+".csv"));
        this.columns = columns;
        i_s = convertFileDataToPrimitiveList(dataIterator.readLine().split("\\|"),columns,schema);
        this.where = where;


    }

    public boolean checkWhereConditionOnLine(String line){
//        System.out.println("hiiiiiii");
        boolean lineFilter = false;
        try {
            int count = 0;
            loop1: for (int i = 0; i < whereList.size(); i++) {
                Expression expression = whereList.get(i);
                PrimitiveValue result = getPrimitiveValueUsingEval(line, expression);
                if (result.toBool()) {
                    count++;
                } else {
                    lineFilter = false;
                    break loop1;
                }
            }

            if (count == whereList.size()) {
                lineFilter = true;
            }
        }
        catch (Exception e){}
        return lineFilter;
    }

    public PrimitiveValue getPrimitiveValueUsingEval(String line, Expression whereExpression){
        try {
            Eval eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) throws SQLException {
                    String[] fields = line.split("\\|");
                    String value = fields[schema.get(column.getTable().getName() != null ? column.getTable().getName() + "." + column.getColumnName() : column.getColumnName()).index];
                    String datatype = schema.get(column.getTable().getName() != null ? column.getTable().getName() + "." + column.getColumnName() : column.getColumnName()).datatype;
                    if (datatype.equalsIgnoreCase("int"))
                        return new LongValue(value);
                    if (datatype.equalsIgnoreCase("string") || datatype.substring(0, 4).equalsIgnoreCase("char") || datatype.equalsIgnoreCase("varchar"))
                        return new StringValue(value);
                    if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("double"))
                        return new DoubleValue(value);
                    if (datatype.equalsIgnoreCase("date"))
                        return new DateValue(value);
//
                    return null;
                }
            };

            PrimitiveValue result = eval.eval(whereExpression);
            return result;
        }
        catch (Exception e){}
        return null;
    }

    public void getAllRecordsForOneLine(String fields[]){
        int pointers[] = new int[joinColumnsIndex.size()];
        int size[] = new int[joinColumnsIndex.size()];
        int incrementPointer = hash_index.size()-1;
        ArrayList<Integer> indexPointer;
        boolean flag;
        int count;
        int fileDataCounter = 0;
        ArrayList<PrimitiveValue> fileprimitiveValueList = new ArrayList();
        ArrayList<PrimitiveValue> inmemoryprimitiveValueList = new ArrayList<PrimitiveValue>();

        for(int i=0; i<pointers.length; i++){
            pointers[i] = 0;
        }

        loop: while (true) {
            fileprimitiveValueList = convertFileDataToPrimitiveList(fields, columns, schema);
            fileDataCounter = 0;
            flag = false;
            count =0;
            for (int i = 0; i < hash_index.size(); i++) {
                PrimitiveValue joinColumnValue = fileprimitiveValueList.get(joinColumnsIndex.get(i));
                HashMap<PrimitiveValue, ArrayList> current = hash_index.get(i);
                if (current.containsKey(joinColumnValue)) {
                    indexPointer = current.get(joinColumnValue);

                    size[i] = indexPointer.size();

                    int currentIndexPointer = indexPointer.get(pointers[i]);
                    int fileDataCounter1 = fileDataCounter++;
                    inmemoryprimitiveValueList = joinData.get(fileDataCounter1).get(currentIndexPointer);
                    fileprimitiveValueList.addAll(inmemoryprimitiveValueList);
                }
                else{
                    break loop;
                }
            }

            joinedRecords.add(fileprimitiveValueList);

            int j;
            for(j=hash_index.size()-1; j>=0; j--){
                if(pointers[j]== (size[j]-1)){
                    count++;
                    flag=true;
                    pointers[j]=0;
                }
                else{
                    break;
                }
            }

            if(!(count == hash_index.size())) {
                if (flag == true) {
                    pointers[j]++;
                    //incrementPointer = j+1;
                } else {
                    pointers[incrementPointer]++;
                }
            }
            else{
                break loop;
            }
        }


//        System.out.println("hiiiiii");
//        for(int i=0; i<joinedRecords.size(); i++){
//            System.out.println(joinedRecords.get(i));
//        }
    }

    /***hash index in mem***/
    public MyIterator(ArrayList<HashMap<PrimitiveValue,ArrayList>> hash_index,ArrayList<HashMap<Integer,ArrayList<PrimitiveValue>>> file_data, String file,ArrayList<String> columns,HashMap<String, DBschema> schema,ArrayList<Integer> joinColumnsIndex, ArrayList<Integer> joinRemovalIndex, ArrayList<Integer[]> joinAdditionalIndex,HashMap<String, DBschema> joinSchema, List<Expression> whereList) throws IOException {
        this.br_hash = new BufferedReader(new FileReader("/home/shruti/DB/Checkpoint2_on_disk/data/" + file + ".csv"));
        this.schema = schema;
        this.joinSchema = joinSchema;
        this.whereList = whereList;
        this.columns = columns;
        this.joinData = file_data;
        this.hash_index = hash_index;
        this.joinColumnsIndex = joinColumnsIndex;
        this.joinRemovalIndex = joinRemovalIndex;
        this.joinAdditionalIndex = joinAdditionalIndex;

        ArrayList<PrimitiveValue> mergedprimitiveValueList = new ArrayList();
        boolean rowPassed = false;
        int counter = 0;

        //   while(indexPointer<=-1){
        boolean result = false;
        String line = new String();
        while(result==false){
            line = br_hash.readLine();
            result = checkWhereConditionOnLine(line);
        }
        if(result == true){
            String fields[] = line.split("\\|");
            getAllRecordsForOneLine(fields);
        }

        i_s = joinedRecords.get(0);
//        System.out.println("i_s "+ i_s);
    }

    private ArrayList<PrimitiveValue> convertFileDataToPrimitiveList(String[] fields, List<String> columns,HashMap<String, DBschema> schema) {
        // TODO Auto-generated method stub
        ArrayList<PrimitiveValue> fileprimitiveValueList = new ArrayList();
        for (int i = 0; i < fields.length; i++) {
            String value = fields[i];

            String datatype = schema.get(columns.get(i)).datatype;

            if (datatype.equalsIgnoreCase("int"))
                fileprimitiveValueList.add(new LongValue(value));
            else if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("double"))
                fileprimitiveValueList.add(new DoubleValue(value));
            else if (datatype.equalsIgnoreCase("date"))
                fileprimitiveValueList.add(new DateValue(value));
            else if (datatype.equalsIgnoreCase("string") || datatype.substring(0, 4).equalsIgnoreCase("char") || datatype.substring(0,7).equalsIgnoreCase("varchar"))
                fileprimitiveValueList.add(new StringValue(value));
        }

        return fileprimitiveValueList;
    }

    /***on disk btree***/
    public MyIterator(String file, HashMap<String,DBschema> schema, Limit limit,List<String> columns,String tableName) throws IOException {

        this.br = new BufferedReader(new FileReader(file));
        this.schema = schema;
        this.rowCount = 0;
        this.limit = limit;
        this.columns = columns;
        String fields[] = br.readLine().split("\\|");
        ArrayList<PrimitiveValue> primitiveValueList = new ArrayList();
        for (int i = 0; i < fields.length-1; i++) {
            String value = fields[i];
            String datatype = schema.get(columns.get(i)).datatype;

            if (datatype.equalsIgnoreCase("int"))
                primitiveValueList.add(new LongValue(value));
            else if (datatype.equalsIgnoreCase("string") || datatype.substring(0, 4).equalsIgnoreCase("char") || datatype.equalsIgnoreCase("varchar"))
                primitiveValueList.add(new StringValue(value));
            else if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("double"))
                primitiveValueList.add(new DoubleValue(value));
            else if (datatype.equalsIgnoreCase("date"))
                primitiveValueList.add(new DateValue(value));
        }
        i_s = primitiveValueList;


    }

    /***on disk files***/
    public MyIterator(ArrayList<String> files, HashMap<Integer,HashMap<String,DBschema>> schemas, ArrayList<Integer> joinColumnsIndexOnDisk, ArrayList<String> columnNames, ArrayList<List<String>> columns) {
        try {

            this.bufferedReader1 = new BufferedReader(new FileReader(files.get(0)));
            this.bufferedReader2 = new BufferedReader(new FileReader(files.get(1)));
            this.schemas = schemas;
            this.joinColumnsIndexOnDisk = joinColumnsIndexOnDisk;
            this.columnNames = columnNames;
            this.columns3 = columns;
            this.joinDataOnDisk = new ArrayList<>();
            this.joinedRecordsOnDisk = new ArrayList<>();
            for (int i = 2; i < schemas.size(); i++) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(files.get(i)));
                this.bufferedReaderArrayList.add(bufferedReader);
            }
            getNextRecord(schemas, joinColumnsIndexOnDisk, columnNames, columns3);
            i_s = joinedRecordsOnDisk.get(0);
//            System.out.println("i_s1 "+i_s);

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public PrimitiveValue getPrimmitiveValue(String fieldValue, String dataType){
        PrimitiveValue primitiveValue = null;
        if (dataType.equalsIgnoreCase("int")) {
            primitiveValue = new LongValue(fieldValue);
        }
        else if (dataType.equalsIgnoreCase("string") || dataType.substring(0, 4).equalsIgnoreCase("char") || dataType.equalsIgnoreCase("varchar")) {
            primitiveValue = new StringValue(fieldValue);
        }
        else if (dataType.equalsIgnoreCase("decimal") || dataType.equalsIgnoreCase("double")) {
            primitiveValue = new DoubleValue(fieldValue);
        }
        else if (dataType.equalsIgnoreCase("date")) {
            primitiveValue = new DateValue(fieldValue);
        }
        return primitiveValue;
    }

    public void getNextRecord(HashMap<Integer,HashMap<String,DBschema>> schemas, ArrayList<Integer> joinColumnsIndexOnDisk, ArrayList<String> columnNames, ArrayList<List<String>> columns3){
        try {
            HashMap<String,DBschema> schema1 = schemas.get(0);
            HashMap<String,DBschema> schema2 = schemas.get(1);
            List<String> columns1 = columns3.get(0);
            List<String> columns2 = columns3.get(1);
            boolean flag = true;
            int index1 = joinColumnsIndexOnDisk.get(0);
            int index2 = schema2.get(columnNames.get(0)).index;
            ArrayList<PrimitiveValue> primitiveValueList = new ArrayList();
            String line1 = bufferedReader1.readLine();
            String line2 = bufferedReader2.readLine();

            while ((flag == true) && ((line1 != null) && (line2 != null))) {
                String fields1[] = line1.split("\\|");
                String fields2[] = line2.split("\\|");
                if (fields1[index1].compareTo(fields2[index2]) == 0) {
                    for (int i = 0; i < fields1.length; i++) {
//                        if (i != index1) {
                        String fieldValue = fields1[i];
                        String dataType = schema1.get(columns1.get(i)).datatype;
                        PrimitiveValue primitiveValue = getPrimmitiveValue(fieldValue, dataType);
                        primitiveValueList.add(primitiveValue);
                        // }
                    }
                    for (int i = 0; i < fields2.length; i++) {
                        String fieldValue = fields2[i];
                        String dataType = schema2.get(columns2.get(i)).datatype;
                        PrimitiveValue primitiveValue = getPrimmitiveValue(fieldValue, dataType);
                        primitiveValueList.add(primitiveValue);
                    }
                    flag = false;
                } else if (fields1[index1].compareTo(fields2[index2]) > 0) {
                    line2 = bufferedReader2.readLine();
                } else if (fields1[index1].compareTo(fields2[index2]) < 0) {
                    line1 = bufferedReader1.readLine();
                }
            }
            if (primitiveValueList.isEmpty()) {
                i_s = null;
            }
            else {
                getAllRecordsForOneLineOnDisk(schemas, joinColumnsIndexOnDisk, columnNames, columns3, primitiveValueList);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

    public void getAllRecordsForOneLineOnDisk(HashMap<Integer, HashMap<String,DBschema>> schemas, ArrayList<Integer> joinColumnsIndexOnDisk, ArrayList<String> columnNames, ArrayList<List<String>> columns, ArrayList<PrimitiveValue> primitiveValueList) {
        try {
            HashMap<Integer, ArrayList<PrimitiveValue>> hashMap = new HashMap<>();
            hashMap.put(0, primitiveValueList);
            this.joinDataOnDisk.add(0, hashMap);
            ArrayList<Integer> newCheckPointList1 = new ArrayList<>();
            newCheckPointList1.add(0);

            ArrayList<PrimitiveValue> tempPrimitiveValueList = new ArrayList();
            ArrayList<ArrayList<Integer>> linesCheckPoint = new ArrayList<>();
            linesCheckPoint.add(newCheckPointList1);

            int count = 0;
            int checkPoint = -1;
            boolean readLine[] = new boolean[bufferedReaderArrayList.size()];
            for (int i = 0; i < bufferedReaderArrayList.size(); i++) {
                readLine[i] = true;
            }

            for (int i = 1; i < columnNames.size(); i++) {
                HashMap<Integer, ArrayList<PrimitiveValue>> currentHashMap = joinDataOnDisk.get(i - 1);
                count = 0;
                ArrayList<Integer> newCheckPointList = new ArrayList<>();
                HashMap<Integer, ArrayList<PrimitiveValue>> hmap = new HashMap<Integer, ArrayList<PrimitiveValue>>();
                String line = new String();
                int tp = 0;
                for (int j = 0; j < currentHashMap.size(); j++) {
                    checkPoint = count;
                    newCheckPointList.add(checkPoint);
                    if(tp!=0) {
                        linesCheckPoint.remove(i);
                    }
                    linesCheckPoint.add(i, newCheckPointList);
                    tempPrimitiveValueList = currentHashMap.get(j);
                    PrimitiveValue joinColumnValue = tempPrimitiveValueList.get(joinColumnsIndexOnDisk.get(i));
                    BufferedReader bufferedReader = bufferedReaderArrayList.get(i - 1);
                    int index = schemas.get(i + 1).get(columnNames.get(i)).index;

                    while (line != null) {
                        if (readLine[i - 1] == true) {
                            line = bufferedReader.readLine();
                        }
                        String fields[] = line.split("\\|");
                        ArrayList<PrimitiveValue> current = convertFileDataToPrimitiveList(fields, columns.get(i+1), schemas.get(i+1));
                        readLine[i - 1] = true;
                        boolean flag = false;
                        if (joinColumnValue.equals(current.get(index))) {
                            hmap.put(count++, current);
                            this.joinDataOnDisk.add(i, hmap);
                            flag = true;
                        }
                        if(flag==true){
                            readLine[i - 1] = false;
                            break;
                        }

                    }
                }
            }

            int pointers[] = new int[linesCheckPoint.size()];
            for(int i=0; i<linesCheckPoint.size(); i++){
                pointers[i] = 0;
            }

            int count2 = 1;
            loop: while(true) {
                ArrayList<PrimitiveValue> fileprimitiveValueList = new ArrayList();
                for (int i = 0; i < linesCheckPoint.size(); i++) {
//                    HashMap<Integer, ArrayList<PrimitiveValue>> currentHashMap = joinDataOnDisk.get(i);
//                    ArrayList<Integer> currentCPList = linesCheckPoint.get(i);
                    if(!(pointers[i]== joinDataOnDisk.get(i).size())) {
                        fileprimitiveValueList.addAll(joinDataOnDisk.get(i).get(pointers[i]));
                    }
                    else{
                        break loop;
                    }
                }
                joinedRecordsOnDisk.add(fileprimitiveValueList);
                int j;
                for(j=joinDataOnDisk.size()-1; j>=0; j--){
                    if(linesCheckPoint.get(j).size()>count2) {
                        if (pointers[j] == (linesCheckPoint.get(j).get(count2) - 1)) {
                            pointers[j]++;
                            count2++;
                        }
                    }
                    else{
                        pointers[j]++;
                        break;
                    }
                }
            }
        }
        catch(Exception e){
//            System.out.println(e.getMessage());
        }
    }


//        public void getAllRecordsForOneLineOnDisk(HashMap<Integer, HashMap<String,DBschema>> schemas, ArrayList<Integer> joinColumnsIndexOnDisk, ArrayList<String> columnNames, ArrayList<List<String>> columns, ArrayList<PrimitiveValue> primitiveValueList) {

//    try{
//        int pointers[] = new int[columnNames.size() - 1];
//        int size[] = new int[columnNames.size() - 1];
//        int incrementPointer = joinColumnsIndexOnDisk.size() - 2;
//        ArrayList<Integer> indexPointer = new ArrayList<>();
//        boolean flag;
//        int count;
//        int fileDataCounter = 0;
//        ArrayList<PrimitiveValue> fileprimitiveValueList = new ArrayList();
//        ArrayList<PrimitiveValue> inmemoryprimitiveValueList = new ArrayList<PrimitiveValue>();
//
//        for (int i = 0; i < pointers.length; i++) {
//            pointers[i] = 0;
//            size[i] = 0;
//        }
//
//        loop:
//        while (true) {
//            fileDataCounter = 0;
//            flag = false;
//            count = 0;
//            for (int i = 1; i < columnNames.size(); i++) {
//                PrimitiveValue joinColumnValue = primitiveValueList.get(joinColumnsIndexOnDisk.get(i));
//                BufferedReader bufferedReader = bufferedReaderArrayList.get(i-1);
//                int index = schemas.get(i).get(columnNames.get(i)).index;
//                while(true){
//                    String fields[] = bufferedReader.readLine().split("\\|");
//                    ArrayList<PrimitiveValue> current = convertFileDataToPrimitiveList(fields, columns.get(i), schemas.get(i));
//                    int count1 =0;
//                    while(joinColumnValue.equals(current.get(index))) {
//                        indexPointer.add(count1);
//                        size[i-1]++;
//                        count1++;
//                        String fields1[] =bufferedReader.readLine().split("\\|");
//                        current = convertFileDataToPrimitiveList(fields1, columns.get(i), schemas.get(i));
//                    }
//                }
//                if (current.containsKey(joinColumnValue)) {
//                    indexPointer = current.get(joinColumnValue);
//
//                    size[i] = indexPointer.size();
//
//                    int currentIndexPointer = indexPointer.get(pointers[i]);
//                    int fileDataCounter1 = fileDataCounter++;
//                    inmemoryprimitiveValueList = joinData.get(fileDataCounter1).get(currentIndexPointer);
//                    fileprimitiveValueList.addAll(inmemoryprimitiveValueList);
//                } else {
//                    break loop;
//                }
//            }
//
//            joinedRecords.add(fileprimitiveValueList);
//
//            int j;
//            for (j = hash_index.size() - 1; j >= 0; j--) {
//                if (pointers[j] == (size[j] - 1)) {
//                    count++;
//                    flag = true;
//                    pointers[j] = 0;
//                } else {
//                    break;
//                }
//            }
//
//            if (!(count == hash_index.size())) {
//                if (flag == true) {
//                    pointers[j]++;
//                    //incrementPointer = j+1;
//                } else {
//                    pointers[incrementPointer]++;
//                }
//            } else {
//                break loop;
//            }
//        }
//
//
////        System.out.println("hiiiiii");
////        for(int i=0; i<joinedRecords.size(); i++){
////            System.out.println(joinedRecords.get(i));
////        }
//    }
//    catch(Exception exception){}
//    }

//    public void newCrossJoinRecord(String line1, HashMap<String,DBschema> schema1, HashMap<String,DBschema> schema2, String columnName1, String columnName2, List<String> columns1,List<String> columns2){
//        try {
//            boolean flag = true;
//            int index1 = schema1.get(columnName1).index;
//            int index2 = schema2.get(columnName2).index;
//            ArrayList<PrimitiveValue> primitiveValueList = new ArrayList();
//            String line2 = new String();
//            String fields1[] = line1.split("\\|");
//
//            while ((flag == true) && (line2 != null)) {
//                line2 = bufferedReader4.readLine();
//                String fields2[] = line2.split("\\|");
//                if (fields1[index1].compareTo(fields2[index2]) == 0) {
//                    for (int i = 0; i < fields1.length; i++) {
//                        String fieldValue = fields1[i];
//                        String dataType = schema1.get(columns1.get(i)).datatype;
//                        PrimitiveValue primitiveValue = getPrimmitiveValue(fieldValue, dataType);
//                        primitiveValueList.add(primitiveValue);
//                    }
//                    for (int i = 0; i < fields2.length; i++) {
//                        String fieldValue = fields2[i];
//                        String dataType = schema2.get(columns2.get(i)).datatype;
//                        PrimitiveValue primitiveValue = getPrimmitiveValue(fieldValue, dataType);
//                        primitiveValueList.add(primitiveValue);
//                    }
//                    flag = false;
//                }
//            }
//            if (primitiveValueList.isEmpty()) {
//                i_s = null;
//            } else {
//                i_s = primitiveValueList;
//            }
//        }
//        catch(Exception exception){}
//    }

    /***datatable***/
    public MyIterator(ArrayList<ArrayList<PrimitiveValue>> rowsValue){
        rows=rowsValue;
        rowsIterator=rows.iterator();
        if(rowsIterator.hasNext())
            i_s = rowsIterator.next();

    }


    @Override
    public boolean hasNext() {

        if(i_s==null)
            return false;
        return true;
    }

    @Override
    public ArrayList<PrimitiveValue> next() {
        ArrayList<PrimitiveValue> s=i_s;

        if(rowsIterator!=null){

            if(rowsIterator.hasNext())
                i_s= rowsIterator.next();
            else
                i_s=null;

        }
        else if(dataIterator!=null)
        {
            i_s=null;
            String line = new String();
            if(line!=null) {

                while (line!=null) {
                    ArrayList<PrimitiveValue> value = new ArrayList<>();
                    if(this.limit!=null)
                        if(rowCount>=this.limit.getRowCount()+1)
                        {
                            i_s=null;
                            break;
                        }

                    try {
                        line = dataIterator.readLine();
                        if(line==null)
                            break;
                        value = convertFileDataToPrimitiveList(line.split("\\|"),columns,schema);

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    ArrayList<PrimitiveValue> temp = value;
                    Eval eval = new Eval() {
                        @Override
                        public PrimitiveValue eval(Column column) throws SQLException {

                            return temp.get(schema.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index);
                        }
                    };

                    boolean where_flag = true;
                    try {
                        PrimitiveValue result = eval.eval(where);
                        if(!result.toBool())
                        {
                            where_flag = false;
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }


                    if(where_flag==true)
                    {
                        i_s = value;
                        break;
                    }
                }
            }
            else
                i_s=null;
        }

        else if(br!=null)
        {
            i_s=null;

            String line;
            try {
                while ((line=br.readLine())!=null) {

                    if(this.limit!=null)
                        if(rowCount>=this.limit.getRowCount())
                        {
                            i_s=null;
                            break;
                        }

                    String line_eval = line;
                    Eval eval = new Eval() {
                        @Override
                        public PrimitiveValue eval(Column column) throws SQLException {
                            String[] fields = line_eval.split("\\|");
                            String value = fields[schema.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index];
                            String datatype = schema.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).datatype;
                            if (datatype.equalsIgnoreCase("int"))
                                return new LongValue(value);
                            if (datatype.equalsIgnoreCase("string") || datatype.substring(0,4).equalsIgnoreCase("char") || datatype.equalsIgnoreCase("varchar"))
                                return new StringValue(value);
                            if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("double"))
                                return new DoubleValue(value);
                            if (datatype.equalsIgnoreCase("date"))
                                return new DateValue(value);
//
                            return null;
                        }
                    };

//                    boolean where_flag = true;
//                    for(int i=0;i<where.size();i++)
//                    {
//                        try {
//                            PrimitiveValue result = eval.eval(where.get(i));
//                            if(!result.toBool())
//                            {
//                                where_flag = false;
//                                break;
//                            }
//                        } catch (SQLException e) {
//                            e.printStackTrace();
//                        }
//                    }

//                    if(where_flag==true)
//                    {
                    String[] fields = line.split("\\|");
                    ArrayList<PrimitiveValue> primitiveValueList = new ArrayList();
                    for (int i = 0; i < fields.length-1; i++) {
                        String value = fields[i];
                        String datatype = schema.get(columns.get(i)).datatype;

                        if (datatype.equalsIgnoreCase("int"))
                            primitiveValueList.add(new LongValue(value));
                        else if (datatype.equalsIgnoreCase("string") || datatype.substring(0, 4).equalsIgnoreCase("char") || datatype.equalsIgnoreCase("varchar"))
                            primitiveValueList.add(new StringValue(value));
                        else if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("double"))
                            primitiveValueList.add(new DoubleValue(value));
                        else if (datatype.equalsIgnoreCase("date"))
                            primitiveValueList.add(new DateValue(value));
                    }
                    i_s = primitiveValueList;

//                        i_s = line;
                    break;
//                    }
                }

//            }
//            else
//                i_s=null;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        else if(br_hash!=null){
            try {
                //line = br_hash.readLine();

                counterForNext++;
                if(counterForNext <joinedRecords.size()){
                    i_s = joinedRecords.get(counterForNext);
                }
                else {

                    joinedRecords.clear();
                    counterForNext = 0;
                    loop1: while (joinedRecords.isEmpty()) {
                        boolean result = false;
                        String line = new String();
                        while (result == false) {
                            line = br_hash.readLine();
                            if(line!=null) {
                                result = checkWhereConditionOnLine(line);
                            }
                            else{
                                i_s = null;
                                break loop1;
                            }
                        }
                        if (result == true) {
                            String fields[] = line.split("\\|");
                            getAllRecordsForOneLine(fields);
                        }
                    }
                    if (!joinedRecords.isEmpty()) {
                        i_s = joinedRecords.get(0);
                    }
//                        }
//                        else{
//                            i_s = null;
//                        }
                }
//                System.out.println("i_s "+ i_s);
//
//                    Integer indexPointer =-1;
//                    int joincolumnindexCounter = -1;
//                    int fileDataCounter = -1;
//                    ArrayList<PrimitiveValue> fileprimitiveValueList = new ArrayList();
//                    ArrayList <PrimitiveValue> inmemoryprimitiveValueList = new ArrayList<PrimitiveValue>();
//                    ArrayList<PrimitiveValue> mergedprimitiveValueList = new ArrayList();
//                    boolean rowPassed = false;
//                    int counter=0;
//                    boolean nullFlag = false;
//                    whileLabel:
//                    while(indexPointer<=-1){
//
//                        if(line==null)
//                        {
//                            nullFlag = true;
//                            break;
//                        }
//
//                        String fields[] = line.split("\\|");
//                        fileprimitiveValueList = convertFileDataToPrimitiveList(fields,columns,schema);
//
//                        for(HashMap<PrimitiveValue,Integer> current: hash_index){
//                            PrimitiveValue joinColumnValue = fileprimitiveValueList.get(joinColumnsIndex.get(++joincolumnindexCounter));
//                            if(current.containsKey(joinColumnValue)) {
//                                indexPointer = current.get(joinColumnValue);
//                                inmemoryprimitiveValueList = joinData.get(++fileDataCounter).get(indexPointer);
//                                fileprimitiveValueList.addAll(inmemoryprimitiveValueList);
//                                counter++;
//                            }else{
//                                indexPointer = -1;
//                                joincolumnindexCounter=-1;
//                                fileDataCounter=-1;
//                                line = br_hash.readLine();
//                                break;
//                            }
//                        }
//
//                        if(counter == hash_index.size()){
//
//                            /*ArrayList<PrimitiveValue> temp = fileprimitiveValueList;
//                            Eval eval = new Eval() {
//                                @Override
//                                public PrimitiveValue eval(Column column) throws SQLException {
//
//
//                                    return temp.get(joinSchema.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index);
//                                }
//                            };
//
//                            try {
//                                PrimitiveValue result = eval.eval(where);
//                                if(!result.toBool())
//                                {
//                                    rowPassed = false;
//                                    indexPointer = -1;
//                                    joincolumnindexCounter = -1;
//                                    fileDataCounter = -1;
//                                    continue;
//                                }
//                            } catch (SQLException e) {
//                                e.printStackTrace();
//                            }*/
//                            rowPassed = true;
//                        }
//
//                        if(rowPassed){
//                            for(Integer[] jAddIndex : joinAdditionalIndex){
//                                PrimitiveValue key1 = fileprimitiveValueList.get(jAddIndex[0]);
//                                PrimitiveValue key2 = fileprimitiveValueList.get(jAddIndex[1]);
//                                if(key1.equals(key2)){
//                                    break;
//                                }else{
//                                    indexPointer = -1;
//                                    joincolumnindexCounter = -1;
//                                    fileDataCounter=-1;
//                                    line = br_hash.readLine();
//                                    continue whileLabel;
//                                }
//                            }
//
//                        }
//                        //line = br_hash.readLine();
//
//                    }
//                    int track = 0;
//                    //removing duplicate columns from the joined table
//                    /*for(Integer rIndex : joinRemovalIndex){
//                        fileprimitiveValueList.remove(rIndex-track);
//                        track++;
//                    }*/
//
//                    for(PrimitiveValue pv : fileprimitiveValueList ){
//                        mergedprimitiveValueList.add(pv);
//                    }
//
//                    if(nullFlag)
//                        i_s = null;
//                    else
//                        i_s = mergedprimitiveValueList;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        else if((bufferedReader1!=null) && (bufferedReader2!=null)) {

            countReturnRecordsOnDisk++;
            if (countReturnRecordsOnDisk < joinedRecordsOnDisk.size()) {
                i_s = joinedRecordsOnDisk.get(countReturnRecordsOnDisk);
            }
            else {
                joinedRecordsOnDisk.clear();
                countReturnRecordsOnDisk = 0;
                getNextRecord(schemas, joinColumnsIndexOnDisk, columnNames, columns3);
                if(!joinedRecordsOnDisk.isEmpty()){
                    i_s = joinedRecordsOnDisk.get(0);
                }
            }
//            System.out.println("i_s1 " + i_s);
        }

        rowCount++;
        return s;
    }

}