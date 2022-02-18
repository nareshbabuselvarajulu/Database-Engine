//package dubstep;
/***clear code to avoid redundancy***/
import com.sun.org.apache.xpath.internal.operations.Equals;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.*;

import java.io.*;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by shruti on 15/4/17.
 */
public class InMem {

    public static HashMap<String,HashMap<Integer,ArrayList<PrimitiveValue>>> file_data = new HashMap<>();
    public static HashMap<String,ArrayList<String>> table_columns = new HashMap<>();
    public static HashMap<String,HashMap<String,HashMap<PrimitiveValue,ArrayList>>> index_hashes = new HashMap<>();
    public static HashMap<String,ArrayList<Integer>> indices = new HashMap<>();
    public static HashMap<String,HashMap<String,DBschema>> schema = new HashMap<>();
    public static HashMap<String,String> parserToQueryDataType = new HashMap<>();


    public static void main() throws ParseException, SQLException, IOException {

        setParserToQueryDataType();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("$>");
        while(true)
        {
            try {
                String ddl;
                StringBuilder ddl_temp =new StringBuilder();
                while(true)
                {
                    ddl= br.readLine();
                    ddl_temp.append(" "+ddl);
                    if(ddl.contains(";"))
                        break;
                }
                ddl = ddl_temp.toString();
                StringReader input = new StringReader(ddl);
                CCJSqlParser pm = new CCJSqlParser(input);
                net.sf.jsqlparser.statement.Statement query = pm.Statement();


                if(query instanceof CreateTable) {
                    long startTime = System.currentTimeMillis();
                    CreateTable ct = processDDL(ddl);

                    /***remaining indexes***/
                    List<Index> indexList = ct.getIndexes();
                    List<String> indexColumns = new ArrayList<>();
                    if (indexList != null)
                        for (int i = 0; i < indexList.size(); i++)
                            indexColumns.addAll(indexList.get(i).getColumnsNames());

                    for (int i = 0; i < indexColumns.size(); i++)
                        indexColumns.set(i, ct.getTable().getName() + "." + indexColumns.get(i));

                    List<ColumnDefinition> columnsdefs = ct.getColumnDefinitions();
                    ArrayList<String> columns = new ArrayList<>();
                    for (int i = 0; i < columnsdefs.size(); i++) {
                        columns.add(ct.getTable().getName() + "." + columnsdefs.get(i).getColumnName());
                        if (columnsdefs.get(i).getColumnSpecStrings() != null)
                            if (!indexColumns.contains(ct.getTable().getName() + "." + columnsdefs.get(i).getColumnName()))
                                indexColumns.add(ct.getTable().getName() + "." + columnsdefs.get(i).getColumnName());
                    }
                    table_columns.put(ct.getTable().getName(), columns);

                    if(!ct.getTable().getName().equals("LINEITEM"))
                    createTree(ct.getTable().getName(), columns, indexColumns);

                }

                else if (query instanceof Select) {
                    long startTime = System.currentTimeMillis();
                    processDML(query);
                    long endTime   = System.currentTimeMillis();
                    long totalTime = endTime - startTime;
//                    System.out.println(totalTime);

                } else {
                    throw new SQLException("I can't understand " + query);
                }

            } catch (Exception e) {
                System.out.println(e.toString());
                e.printStackTrace();
            }
            System.out.print("$>");
        }
    }




    public static void createTree(String tableName, ArrayList<String> columnNames, List<String> listOfIndexedColumns){


        try {
            HashMap<Integer,ArrayList<PrimitiveValue>> data = new HashMap<>();
            FileReader i_fileReader = new FileReader("/home/shruti/DB/Checkpoint2_on_disk/data/" + tableName + ".csv");
            BufferedReader file_br = new BufferedReader(i_fileReader);
            String line = file_br.readLine();
            indices.put(tableName,new ArrayList<>());

            int count = 0;
            ArrayList<PrimitiveValue> primitiveValueList = new ArrayList<>();
            while (line != null) {

                String fields[] = line.split("\\|");
                primitiveValueList = new ArrayList();
                for (int i = 0; i < fields.length; i++) {
                    String value = fields[i];
                    String datatype = schema.get(tableName).get(columnNames.get(i)).datatype;

                    if (datatype.equalsIgnoreCase("int"))
                        primitiveValueList.add(new LongValue(value));
                    else if (datatype.equalsIgnoreCase("decimal") || datatype.equalsIgnoreCase("double"))
                        primitiveValueList.add(new DoubleValue(value));
                    else if (datatype.equalsIgnoreCase("date"))
                        primitiveValueList.add(new DateValue(value));
                    else if (datatype.equalsIgnoreCase("string") || datatype.substring(0, 4).equalsIgnoreCase("char") || datatype.substring(0,7).equalsIgnoreCase("varchar"))
                        primitiveValueList.add(new StringValue(value));
                }

                data.put(count, primitiveValueList);
                indices.get(tableName).add(count);

                /*** building hash index ***/
                index_hashes.putIfAbsent(tableName,new HashMap<>());
                for (int j = 0; j < listOfIndexedColumns.size(); j++) {
                    index_hashes.get(tableName).putIfAbsent(listOfIndexedColumns.get(j), new HashMap<>());
                    HashMap temp = index_hashes.get(tableName).get(listOfIndexedColumns.get(j));
                    PrimitiveValue currentColumnValue = primitiveValueList.get(schema.get(tableName).get(listOfIndexedColumns.get(j)).index);
                    if (!temp.containsKey(currentColumnValue)) {
                        ArrayList<Integer> multipleIndicesOfSameColumnValue = new ArrayList<>();
                        multipleIndicesOfSameColumnValue.add(count);
                        index_hashes.get(tableName).get(listOfIndexedColumns.get(j)).put(currentColumnValue, multipleIndicesOfSameColumnValue);
                    }
                    else{
                          index_hashes.get(tableName).get(listOfIndexedColumns.get(j)).get(currentColumnValue).add(count);
                    }
                }
                /*** ended building hash index ***/

                line = file_br.readLine();
                count++;

            }
            file_data.put(tableName,data);
        }
        catch(Exception e){
            e.printStackTrace();

        }

    }

    /*** to set the datatype accordingly for the create table query ***/
    private static void setParserToQueryDataType() {
        // TODO Auto-generated method stub
        parserToQueryDataType.put("LONG", "int");
        parserToQueryDataType.put("DOUBLE", "double");
        parserToQueryDataType.put("DECIMAL", "double");
        parserToQueryDataType.put("STRING", "varchar");
        parserToQueryDataType.put("DATE", "date");
    }


    /*** to process create table query and create the DB schema ***/
    public static CreateTable processDDL(String ddl) throws ParseException, net.sf.jsqlparser.parser.ParseException {

        StringReader input = new StringReader(ddl);
        CCJSqlParser pm = new CCJSqlParser(input);
        net.sf.jsqlparser.statement.Statement query = pm.Statement();
        CreateTable ct = (CreateTable) query;
        Table t1 = (Table) ct.getTable();
        String tableName = t1.getName();


        int columnnumber = 0;
        HashMap<String,DBschema> temp = new HashMap<>();
        List<ColumnDefinition> coldeflist = ct.getColumnDefinitions();
        Iterator<ColumnDefinition> iter = coldeflist.iterator();
        while (iter.hasNext()) {
            ColumnDefinition coldef = (ColumnDefinition) iter.next();
            String columndatatype = coldef.getColDataType().toString();
            String columnname = coldef.getColumnName().toString();
            DBschema dbs = new DBschema(columnnumber, columndatatype);
            temp.put(tableName.split("_")[0]+"."+columnname, dbs);
            columnnumber++;
        }
        schema.put(tableName,temp);

        return ct;
    }


    /*** to process the select query by getting the fileIterator from the data table finally created after call to processSelect and iterating row by row ***/
    public static void processDML(Statement query) throws IOException, SQLException, ParseException, net.sf.jsqlparser.parser.ParseException {

        SelectBody tempsb = ((Select) query).getSelectBody();
        SelectBody select_b = ((Select) query).getSelectBody();
        FromItem tempfi = ((PlainSelect) tempsb).getFromItem();

        List<OrderByElement> orderBy = new ArrayList<>();
        if(((PlainSelect) tempsb).getOrderByElements()!=null)
            orderBy.addAll(((PlainSelect) tempsb).getOrderByElements());
        Limit limit = ((PlainSelect) tempsb).getLimit();
        long rowCount = 0;
        if (limit != null)
            rowCount = limit.getRowCount();
        String tableName = null;

        while (true) {
            if (tempfi instanceof Table) {
                tableName = ((Table) tempfi).getName();
                break;
            }
            tempsb = ((SubSelect) tempfi).getSelectBody();
            tempfi = ((PlainSelect) tempsb).getFromItem();
        }


        SelectBody querybody = ((Select) query).getSelectBody();

        MyIterator myIter = null;
        DataTable dt = null, dt1 = null;
        int iterationNumber = 0;
        List<ArrayList<PrimitiveValue>> tableRows = new ArrayList<ArrayList<PrimitiveValue>>();
        List<ArrayList<PrimitiveValue>> resultData = new ArrayList<ArrayList<PrimitiveValue>>();
        HashMap<String, DBschema> schema2 = new HashMap<>();

        do {
            dt = processSelect(querybody, dt1, tableName, 0, iterationNumber++,orderBy,limit);

            ArrayList<ArrayList<PrimitiveValue>> temp = new ArrayList<>();
            schema2 = dt.getSchemaList().get(dt.getTableNames().get(0)).get(dt.getTableNames().get(0));
            if (dt != null) {

                dt1 = dt;
                tableRows = dt.getAllTableData().get(dt.getTableNames().get(0));
                resultData.addAll(tableRows);

                myIter = dt.getFileIterator();

            } else {

                break;
            }

        } while (myIter.hasNext());

        /***orderby***/
            MyComparator_2 comparator_2 = new MyComparator_2(schema2,orderBy);
            Collections.sort(resultData,comparator_2);
        /***orderby***/

        /***limit clause***/
        if(limit==null)
            dt.getAllTableData().put(dt.getTableNames().get(0),resultData);
        else
        {
            List<ArrayList<PrimitiveValue>> resultData1 = new ArrayList<>();
            for(int i=0;i<rowCount && i<resultData.size();i++)
                resultData1.add(resultData.get(i));
            dt.getAllTableData().put(dt.getTableNames().get(0),resultData1);
        }
        /***limit clause***/

        /***diaplaying data***/
        int select =0;
        List<SelectItem> si = ((PlainSelect) select_b).getSelectItems();
        if(si.get(0) instanceof AllColumns ){
            select = schema2.size();
        }
        else
        {
            select = si.size();
        }
        displayDataTable(dt,select);
    }

    /*** recursively process the select clause at each level and creates a datatable ***/
    public static void populateMaps(String key, ArrayList<PrimitiveValue> rowValue, HashMap<String, DBschema> schema1, List<Column> groupby, List<String> groups, HashMap<String,ArrayList<Aggregate>> agg_map, HashMap<String,ArrayList<PrimitiveValue>> col_map,List<SelectExpressionItem> selectExpressionItemList, Expression w) throws SQLException {

        /**define eval**/
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                return rowValue.get(schema1.get(column.getTable()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index);
            }
        };

        /**check for where condition**/
        if(w!=null)
            if(!eval.eval(w).toBool())
                return;

        /**construct the key for group_map hashmap**/
        for (int i = 0; i < groupby.size(); i++) {
            key = key + rowValue.get(schema1.get(groupby.get(i).getTable().getName()+"."+groupby.get(i).getColumnName()).index).toRawString();
        }
        if (!groups.contains(key))
            groups.add(key);

        /**build one answer row as stringbuilder**/
        for (int i = 0; i < selectExpressionItemList.size(); i++) {
            Expression exp = selectExpressionItemList.get(i).getExpression();
            if(exp instanceof Function) {
                /** aggregate map **/
                agg_map.putIfAbsent(key, new ArrayList<Aggregate>());
                if (agg_map.get(key).size()<=i)
                    agg_map.get(key).add(new Aggregate());

                if(((Function) exp).isAllColumns()) {
                    if (((Function) exp).getName().equalsIgnoreCase("count")) {
                        agg_map.get(key).get(i).doFunction(((Function) exp).getName(), null);
                    }
                }
                else
                    agg_map.get(key).get(i).doFunction(((Function) exp).getName(), eval.eval(((Function) exp).getParameters().getExpressions().get(0)));

                /** columns map **/
                col_map.putIfAbsent(key, new ArrayList<PrimitiveValue>());
                if (col_map.get(key).size() <= i)
                    col_map.get(key).add(new StringValue(""));
            }

            else
            {
                /** aggregate map **/
                agg_map.putIfAbsent(key,new ArrayList<Aggregate>());
                if(agg_map.get(key).size()<=i)
                    agg_map.get(key).add(new Aggregate());

                /** columns map **/
                col_map.putIfAbsent(key,new ArrayList<PrimitiveValue>());
                if(col_map.get(key).size()<=i)
                    col_map.get(key).add(new StringValue(""));
                col_map.get(key).set(i,eval.eval(exp));
            }
        }
    }

    public static void buildStringBuilder(List<String> groups, HashMap<String,ArrayList<Aggregate>> agg_map, HashMap<String,ArrayList<PrimitiveValue>> col_map, List<SelectExpressionItem> selectExpressionItemList, List<ArrayList<PrimitiveValue>> data)
    {
        for(int i=0;i<groups.size();i++)
        {
            ArrayList<PrimitiveValue> s = new ArrayList<>();
            for(int j=0;j<selectExpressionItemList.size();j++)
            {
                Expression exp = selectExpressionItemList.get(j).getExpression();
                if (exp instanceof Function)
                {
                    if(agg_map.get(groups.get(i)).size()<=0)
                        continue;
                    else {
                        s.add(agg_map.get(groups.get(i)).get(j).getResult().getPrimitiveValue());
                    }
                }

                else
                {
                    if(col_map.get(groups.get(i)).size()<=0)
                        continue;
                    else
                    {
                        s.add(col_map.get(groups.get(i)).get(j));
                    }
                }
            }
            if(s.size()>0)
                data.add(s);
        }
    }

    public static HashMap<String,DBschema> buildSchema(String basetable,ArrayList<String> joinTables) {
        HashMap<String, DBschema> new_schema = new HashMap<>();
        HashMap<String, DBschema> schema1 = schema.get(basetable);
        List<String> columnlist = new ArrayList<>();
        List<String> datatypes = new ArrayList<>();

        /*** appending the columns from the base table ***/
        while (columnlist.size() <= schema1.size() - 1)
        {
            columnlist.add("");
            datatypes.add("");
        }
        for (String s : schema1.keySet())
        {
            columnlist.set(schema1.get(s).index, s);
            datatypes.set(schema1.get(s).index,schema1.get(s).datatype);
        }

        /*** appending all the columns ***/
        for(int j=0;j<joinTables.size();j++) {
            int size = columnlist.size();
            HashMap<String, DBschema> schema2 = schema.get(joinTables.get(j));
            while (columnlist.size() <= size + schema2.size() - 1)
            {
                columnlist.add("");
                datatypes.add("");
            }
            for (String s : schema2.keySet())
            {
                columnlist.set(schema2.get(s).index+size, s);
                datatypes.set(schema2.get(s).index+size,schema2.get(s).datatype);
            }
        }

        for (int i = 0; i < columnlist.size(); i++) {
            DBschema temp = new DBschema(i, datatypes.get(i));
            new_schema.put(columnlist.get(i), temp);
        }

        return new_schema;

    }

    public static List<Expression> andExpression(Expression expression, List<net.sf.jsqlparser.expression.Expression> andExpressionList){
        if(!expression.equals(null)){
            if(!(expression instanceof AndExpression)){
                andExpressionList.add(expression);
            }
            else{
                AndExpression andExpression = (AndExpression) expression;
                Expression leftExpression = andExpression.getLeftExpression();
                Expression rightExpression = andExpression.getRightExpression();
                andExpressionList.add(rightExpression);
                andExpression(leftExpression, andExpressionList);
            }
        }
        return andExpressionList;
    }

    public static int lookForIndexInSchema(String currentColumn, String currentTable, ArrayList<String> alreadyJoinedTables){
        int index = schema.get(currentTable).get(currentTable+"."+currentColumn).index;
        for(int i=0; i<alreadyJoinedTables.size(); i++){
            if(alreadyJoinedTables.get(i).equals(currentTable)){
                break;
            }
            else{
                index += schema.get(alreadyJoinedTables.get(i)).size();
            }
        }
        return index;
    }

    public static ArrayList<Expression> buildWhereExpressionList(List<Expression> where, String baseTable)
    {
        ArrayList<Expression> iter_where = new ArrayList<>();
        for(int i=0;i<where.size();i++)
        {
            Expression exp= where.get(i);
            if(exp instanceof Addition)
            {
                Addition exp1 = (Addition) exp;
                boolean flag = true;
                if( exp1.getLeftExpression() instanceof Column )
                    if(!((Column)exp1.getLeftExpression()).getTable().getName().equals(baseTable))
                        flag = false;
                if( exp1.getRightExpression() instanceof Column )
                    if(!((Column)exp1.getRightExpression()).getTable().getName().equals(baseTable))
                        flag = false;

                if(flag)
                    iter_where.add(where.get(i));
            }
            else if(exp instanceof Subtraction)
            {
                Subtraction exp1 = (Subtraction) exp;
                boolean flag = true;
                if( exp1.getLeftExpression() instanceof Column )
                    if(!((Column)exp1.getLeftExpression()).getTable().getName().equals(baseTable))
                        flag = false;
                if( exp1.getRightExpression() instanceof Column )
                    if(!((Column)exp1.getRightExpression()).getTable().getName().equals(baseTable))
                        flag = false;

                if(flag)
                    iter_where.add(where.get(i));
            }
            else if(exp instanceof GreaterThan)
            {
                GreaterThan exp1 = (GreaterThan) exp;
                boolean flag = true;
                if( exp1.getLeftExpression() instanceof Column )
                    if(!((Column)exp1.getLeftExpression()).getTable().getName().equals(baseTable))
                        flag = false;
                if( exp1.getRightExpression() instanceof Column )
                    if(!((Column)exp1.getRightExpression()).getTable().getName().equals(baseTable))
                        flag = false;

                if(flag)
                    iter_where.add(where.get(i));
            }
            else if(exp instanceof GreaterThanEquals)
            {
                GreaterThanEquals exp1 = (GreaterThanEquals) exp;
                boolean flag = true;
                if( exp1.getLeftExpression() instanceof Column )
                    if(!((Column)exp1.getLeftExpression()).getTable().getName().equals(baseTable))
                        flag = false;
                if( exp1.getRightExpression() instanceof Column )
                    if(!((Column)exp1.getRightExpression()).getTable().getName().equals(baseTable))
                        flag = false;

                if(flag)
                    iter_where.add(where.get(i));
            }
            else if(exp instanceof EqualsTo)
            {
                EqualsTo exp1 = (EqualsTo) exp;
                boolean flag = true;
                if( exp1.getLeftExpression() instanceof Column )
                    if(!((Column)exp1.getLeftExpression()).getTable().getName().equals(baseTable))
                        flag = false;
                if( exp1.getRightExpression() instanceof Column )
                    if(!((Column)exp1.getRightExpression()).getTable().getName().equals(baseTable))
                        flag = false;

                if(flag)
                    iter_where.add(where.get(i));
            }
            else if(exp instanceof NotEqualsTo)
            {
                NotEqualsTo exp1 = (NotEqualsTo) exp;
                boolean flag = true;
                if( exp1.getLeftExpression() instanceof Column )
                    if(!((Column)exp1.getLeftExpression()).getTable().getName().equals(baseTable))
                        flag = false;
                if( exp1.getRightExpression() instanceof Column )
                    if(!((Column)exp1.getRightExpression()).getTable().getName().equals(baseTable))
                        flag = false;

                if(flag)
                    iter_where.add(where.get(i));
            }
        }

        return iter_where;
    }

    public static List<Expression> adjustTables(List<Expression> joinList)
    {
        List<Expression> new_order = new ArrayList<>();
        for(int i=joinList.size()-1;i>=0;i--)
        {
            if(joinList.get(i) instanceof EqualsTo) {
                EqualsTo e = (EqualsTo) joinList.get(i);
                if(e.getRightExpression() instanceof Column && e.getLeftExpression() instanceof Column)
                    if (((Column) e.getRightExpression()).getTable().getName().equals("LINEITEM") || ((Column) e.getLeftExpression()).getTable().getName().equals("LINEITEM"))
                    {
                        Expression exp1 = joinList.get(i);
                        if(!((Column) e.getLeftExpression()).getTable().getName().equals("LINEITEM"))
                        {
                            EqualsTo exp = new EqualsTo();
                            exp.setLeftExpression(e.getRightExpression());
                            exp.setRightExpression(e.getLeftExpression());
                            exp1 = exp;
                        }
                        new_order.add(exp1);
                        joinList.remove(i);
                    }
            }
        }

        for(int i=joinList.size()-1;i>=0;i--)
            new_order.add(joinList.get(i));

        return new_order;
    }

    public static DataTable processSelect(SelectBody sb, DataTable dataTable,String tableName,int level,int iterationNumber, List<OrderByElement> orderBy,Limit limit) throws IOException, SQLException, ParseException, net.sf.jsqlparser.parser.ParseException {

        HashMap<String,ArrayList> associatedTables = new HashMap<>();

        PlainSelect ps = (PlainSelect) sb;
        Expression w = ps.getWhere();

        /*** join table data ***/
        /*** fetching join table list ***/

        ArrayList<String> joinTablesList = new ArrayList<>();
        List<Join> joins = ps.getJoins();
        if(!(joins==null)) {
            Iterator<Join> joinIterator = joins.iterator();
            while (joinIterator.hasNext()) {
                Join join = joinIterator.next();
                boolean typeOfJoin = join.isSimple();
                if (typeOfJoin == true) {
                    FromItem rightItem = join.getRightItem();
                    Table table = (Table) rightItem;
                    joinTablesList.add(table.getName());
                }
            }
        }

        /*** end of fetching join table list***/

        /*** separating where conditions***/

        List<net.sf.jsqlparser.expression.Expression> expressionList = new ArrayList<>();
        List<net.sf.jsqlparser.expression.Expression> joinExpressionList = new ArrayList<>();
        List<net.sf.jsqlparser.expression.Expression> normalWhereExpressionList = new ArrayList<>();
        ArrayList<String> orderedColumnNamesInJoin = new ArrayList<String>();
        if (w != null) {
            expressionList = andExpression(w, expressionList);
        }

        List<Expression> andExpressionList = adjustTables(expressionList);

        for (int i = 0; i < andExpressionList.size(); i++) {
            Expression currentExpression = andExpressionList.get(i);
            if (currentExpression instanceof EqualsTo) {
                EqualsTo equalsTo = (EqualsTo) currentExpression;
                Expression leftExpression = equalsTo.getLeftExpression();
                Expression rightExpression = equalsTo.getRightExpression();

                if (leftExpression instanceof Column) {
                    Column leftExpressionColumn = (Column) leftExpression;
                    if (rightExpression instanceof Column) {
                        Column rightExpressionColumn = (Column) rightExpression;
                        if (!(leftExpressionColumn.getTable().getName() == null) && !(rightExpressionColumn.getTable().getName() == null)) {
                            ArrayList<String> tables = new ArrayList<String>();
                            tables.add(leftExpressionColumn.getTable().getName());
                            tables.add(rightExpressionColumn.getTable().getName());
                            associatedTables.put(leftExpressionColumn.getColumnName(), tables);
                            joinExpressionList.add(currentExpression);
                            orderedColumnNamesInJoin.add(leftExpressionColumn.getColumnName());
                        } else {
                            normalWhereExpressionList.add(currentExpression);
                        }
                    } else {
                        normalWhereExpressionList.add(currentExpression);
                    }
                }
            } else {
                normalWhereExpressionList.add(currentExpression);
            }
        }

        /*** end of separating where conditions***/

        /*** end of join table data ***/


        /*** fix for select star***/
        List<SelectItem> si = ps.getSelectItems();
        List<SelectExpressionItem> selectExpressionItemList = new ArrayList<SelectExpressionItem>();
        if(si.get(0) instanceof AllColumns ){
            HashMap<String, DBschema> currentSchema = schema.get(tableName);
            for(String key: currentSchema.keySet()){
                SelectExpressionItem star = new SelectExpressionItem();
                String[] key_split = key.split(".");
                String col_name,col_table;
                if (key_split.length>1)
                {
                    col_table = key_split[0];
                    col_name = key_split[1];
                }
                else
                {
                    col_name = key_split[0];
                    col_table = null;
                }
                Column c = new Column();
                Table t = new Table();
                t.setName(col_table==null ? null : col_table);
                c.setColumnName(col_name);
                c.setTable(t);
                star.setExpression(c);
                selectExpressionItemList.add(star);
            }
        } else{
            selectExpressionItemList=getSelectExpressionItemlist(sb);
        }

        /*** fix for select star***/


        String createdTableName=generateTableName(tableName, level);
        List<Column> groupby = ps.getGroupByColumnReferences();
        if(groupby==null)
            groupby = new ArrayList<>();
        ArrayList<PrimitiveValue> rowValue=null;

        boolean isAggregate=false;


        for(SelectExpressionItem sei:selectExpressionItemList){

            if(sei.getExpression() instanceof Function){
                isAggregate=true;
                break;
            }

        }


        FromItem fi = ps.getFromItem();
        /**for the base table from the file creating a datatable and setting iterator, schema and tablename**/
        if(fi instanceof Table) {
            createdTableName = generateTableName(tableName, level + 1);

            if (dataTable == null) {

                dataTable = new DataTable();

                MyIterator myIter;
                if(joinTablesList.size()>0)
                {
                    ArrayList<String> alreadyJoinedTables = new ArrayList();
                    ArrayList<Integer> fetchIndicesForJoin = new ArrayList<>();
                    ArrayList<Integer> removalIndices = new ArrayList<>();
                    ArrayList<Integer[]> additionalIndex  = new ArrayList<Integer[]>();
                    ArrayList<String> orderedColumnsWithTableNames = new ArrayList<>();
                    int newRemovalIndex = -1;
                    String newColumnName = new String();

                    for(int i=0; i<orderedColumnNamesInJoin.size(); i++) {
                        String currentColumn = orderedColumnNamesInJoin.get(i);
                        ArrayList<String> tablesAssociatedWithCurrentJoinKey = associatedTables.get(currentColumn);
                        String currentTable = tablesAssociatedWithCurrentJoinKey.get(0);
                        boolean flag = false;

                        for (int j = 0; j < alreadyJoinedTables.size(); j++) {
                            String newTable = tablesAssociatedWithCurrentJoinKey.get(1);
                            int count = 0;
                            if ((alreadyJoinedTables.get(j).equals(currentTable)) || alreadyJoinedTables.get(j).equals(newTable)) {
                                count += 1;
                            }
                            if (count == 2) {
                                flag = true;
                                int index1 = lookForIndexInSchema(currentColumn, currentTable, alreadyJoinedTables);
                                int index2 = lookForIndexInSchema(currentColumn, newTable, alreadyJoinedTables);
                                int previousSize1 =0;
                                int previousSize2 = 0;
                                for(int k=0; k<alreadyJoinedTables.size(); k++){
                                    if(alreadyJoinedTables.get(k).equals(currentTable)){
                                        break;
                                    }
                                    else{
                                        previousSize1+= schema.get(alreadyJoinedTables.get(k)).size();
                                    }
                                }
                                for(int k=0; k<alreadyJoinedTables.size(); k++){
                                    if(alreadyJoinedTables.get(k).equals(newTable)){
                                        break;
                                    }
                                    else{
                                        previousSize2+= schema.get(alreadyJoinedTables.get(k)).size();
                                    }
                                }
                                Integer temp[] = new Integer[2];
                                temp[0] = previousSize1 + index1;
                                temp[1] = previousSize2 + index2;
                                Arrays.sort(temp);
                                additionalIndex.add(temp);

                                newRemovalIndex = temp[1];
                                removalIndices.add(newRemovalIndex);
                                newColumnName= currentTable;
                            }
                        }

                        if (flag == false){
                            if (alreadyJoinedTables.isEmpty()) {
                                String newTable = tablesAssociatedWithCurrentJoinKey.get(1);
                                int currentFetchIndex = lookForIndexInSchema(currentColumn, currentTable, alreadyJoinedTables);
                                String tableToAddInSchema = newTable + "." + currentColumn;
                                orderedColumnsWithTableNames.add(tableToAddInSchema);

                                fetchIndicesForJoin.add(currentFetchIndex);
                                alreadyJoinedTables.add(currentTable);
                                alreadyJoinedTables.add(newTable);
                            } else {
                                int k;
                                boolean newTableFlag = false;
                                for (k = 0; k < alreadyJoinedTables.size(); k++) {
                                    if (alreadyJoinedTables.get(k).equals(currentTable)) {
                                        int currentFetchIndex = lookForIndexInSchema(currentColumn, currentTable, alreadyJoinedTables);
                                        fetchIndicesForJoin.add(currentFetchIndex);
                                        String newTable = tablesAssociatedWithCurrentJoinKey.get(1);
                                        alreadyJoinedTables.add(newTable);
                                        newTableFlag=true;
                                        break;
                                    }
                                }

                                if (k == alreadyJoinedTables.size()) {
                                    alreadyJoinedTables.add(currentTable);
                                    String tableToLookIn = tablesAssociatedWithCurrentJoinKey.get(1);
                                    int currentFetchIndex = lookForIndexInSchema(currentColumn, tableToLookIn, alreadyJoinedTables);
                                    fetchIndicesForJoin.add(currentFetchIndex);
                                }


                                if(newTableFlag==false){
                                    String finalTable = tablesAssociatedWithCurrentJoinKey.get(0);
                                    String tableToAddInSchema = finalTable+"."+currentColumn;
                                    orderedColumnsWithTableNames.add(tableToAddInSchema);
                                }
                                else{
                                    String finalTable = tablesAssociatedWithCurrentJoinKey.get(1);
                                    String tableToAddInSchema = finalTable+"."+currentColumn;
                                    orderedColumnsWithTableNames.add(tableToAddInSchema);
                                }
                            }
                        }
                    }


                    for(int i=1; i<alreadyJoinedTables.size(); i++){
                        String currentColumn = orderedColumnNamesInJoin.get(i-1);
                        int previousSize = 0;
                        if(!currentColumn.equals(newColumnName)) {
                            for(int j=0; j<i;j++){
                                previousSize += schema.get(alreadyJoinedTables.get(j)).size();
                            }
                            int removalIndexFromCurrentTable = previousSize + schema.get(alreadyJoinedTables.get(i)).get(alreadyJoinedTables.get(i)+"."+currentColumn).index;
                            removalIndices.add(removalIndexFromCurrentTable);
                        }
                    }
                    Collections.sort(removalIndices);

                    String baseTable = alreadyJoinedTables.get(0);
                    alreadyJoinedTables.remove(0);
                    /*** build new schema ***/
                    HashMap<String,DBschema> new_schema = buildSchema(baseTable,alreadyJoinedTables);
                    HashMap<String, HashMap<String, DBschema>> currentSchema = new HashMap<String, HashMap<String, DBschema>>();
                    currentSchema.put(createdTableName, new_schema);
                    dataTable.getSchemaList().put(createdTableName, currentSchema);

                    /*** build the arguments to be passed to the iterator***/
                    ArrayList<HashMap<Integer,ArrayList<PrimitiveValue>>> joinTableData = new ArrayList<>();
                    ArrayList<HashMap<PrimitiveValue,ArrayList>> joinTableIndex = new ArrayList<>();
                    for(int i=0;i<alreadyJoinedTables.size();i++)
                    {
                        joinTableData.add(file_data.get(alreadyJoinedTables.get(i)));
                        /*** get only column name form orderedcolumns list ***/
                        joinTableIndex.add(index_hashes.get(alreadyJoinedTables.get(i)).get(orderedColumnsWithTableNames.get(i)));
                    }

                    /***where list for the iterator***/
                    List<Expression> iter_where = buildWhereExpressionList(normalWhereExpressionList,baseTable);


                    myIter = new MyIterator(joinTableIndex,joinTableData,baseTable,table_columns.get(baseTable),schema.get(baseTable),fetchIndicesForJoin,removalIndices,additionalIndex,new_schema,iter_where);
                }
                else
                    myIter = new MyIterator(tableName,table_columns.get(tableName),schema.get(tableName),limit,w);

                dataTable.setFileIterator(myIter);

                dataTable.getMyIter().put(createdTableName, myIter);


                /***********Setting schema**********/
                if(joinTablesList.size()==0) {
                    HashMap<String, HashMap<String, DBschema>> currentSchema = new HashMap<String, HashMap<String, DBschema>>();

                    currentSchema.put(createdTableName, schema.get(tableName));

                    dataTable.getSchemaList().put(createdTableName, currentSchema);
                }

                /************Setting tableName***********/

                dataTable.getTableNames().put(level, createdTableName);
            }



            if (isAggregate) {

                if(iterationNumber>0)
                    return null;

                /**initialize variables for aggregate operation**/
                HashMap<String, String> group_map = new HashMap<>();
                List<String> groups = new ArrayList<>();
                groups.add("");
                group_map.put("", "");
                String key;
                List<ArrayList<PrimitiveValue>> data = new ArrayList<>();
                List<ArrayList<PrimitiveValue>> data1 = new ArrayList<>();
                List<String> dataTypes = new ArrayList<>();
                HashMap<String, DBschema> schema1 = dataTable.getSchemaList().get(createdTableName).get(createdTableName);
                MyIterator iter1 = dataTable.getMyIter().get(createdTableName);

                HashMap<String,ArrayList<PrimitiveValue>> col_map = new HashMap<>();
                col_map.put("", new ArrayList<>());
                HashMap<String,ArrayList<Aggregate>> agg_map = new HashMap<>();
                agg_map.put("", new ArrayList<>());

                /**iterate over the whole data (file in this case)**/

                while (iter1.hasNext()) {
                    try {

                        key = "";
                        rowValue = iter1.next();

                        populateMaps(key,rowValue,schema1,groupby,groups,agg_map,col_map,selectExpressionItemList,w);

                    }catch(Exception e)
                    {
                        e.printStackTrace();
                        break;
                    }
                }

                /***populating datatypes***/
                ArrayList<PrimitiveValue> rowValue_eval = rowValue;
                Eval eval = new Eval() {
                    @Override
                    public PrimitiveValue eval(Column column) throws SQLException {
                        return rowValue_eval.get(schema1.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index);
                    }
                };
                for(int i=0;i<selectExpressionItemList.size();i++)
                {
                    Expression exp = selectExpressionItemList.get(i).getExpression();
                    if(exp instanceof Function)
                    {
                        if(((Function) exp).isAllColumns())
                            dataTypes.add("LONG");
                        else
                            dataTypes.add(eval.eval(((Function) exp).getParameters().getExpressions().get(0)).getType().toString());
                    }
                    else
                    {
                        dataTypes.add(eval.eval((Column) exp).getType().toString());
                    }

                }

                /**adding the data to a stringbuilder list**/
                buildStringBuilder(groups,agg_map,col_map,selectExpressionItemList,data);

                /***check for empty result***/
                if(data.size()==0 && groupby.size()==0)
                {
                    ArrayList<PrimitiveValue> data_line = new ArrayList<>();
                    for(int i=0;i<selectExpressionItemList.size();i++)
                    {
                        Expression exp = selectExpressionItemList.get(i).getExpression();
                        if(exp instanceof Function)
                        {
                            if(((Function) exp).isAllColumns())
                                data_line.add(new LongValue("0"));
                            else
                                data_line.add(new StringValue(""));
                        }
                        else
                            data_line.add(new StringValue(""));
                    }
                    data.add(data_line);
                }

                /**SETTING THE DATATABLE FOR CURRENT LEVEL**/
                String current_createdTableName=generateTableName(tableName,level);
                getCreateQuery(current_createdTableName,selectExpressionItemList, dataTypes);
                dataTable.getTableNames().put(level, current_createdTableName);

                /***********Setting schema**********/
                HashMap<String,HashMap<String,DBschema>> currentSchema;
                if((currentSchema=dataTable.getSchemaList().get(current_createdTableName))==null){
                    currentSchema=new HashMap<>();
                }
                currentSchema.put(current_createdTableName,schema.get(current_createdTableName));
                dataTable.getSchemaList().put(current_createdTableName,currentSchema);

                /***********Setting data**********/
                dataTable.getAllTableData().put(current_createdTableName, data);


                /***********Setting Iterator**********/
                MyIterator iter;
                iter=new MyIterator((ArrayList<ArrayList<PrimitiveValue>>)data);
                dataTable.getMyIter().put(current_createdTableName, iter);

                /***********Setting TableName**********/
                dataTable.getTableNames().put(level, current_createdTableName);

                return dataTable;

            }
            else {

                rowValue = dataTable.getMyIter().get(createdTableName).next();

                return columnQuery(tableName, rowValue,selectExpressionItemList, level, dataTable,w);
            }

        }

        /**for nested select case recursively iterate till reaching base table**/
        if(isAggregate){

            if(iterationNumber>0)
                return null;

            /**initialize variables for aggregate operation**/
            HashMap<String, String> group_map = new HashMap<>();
            List<String> groups = new ArrayList<>();
            groups.add("");
            group_map.put("", "");
            String key;
            List<String> dataTypes = new ArrayList<>();

            /**datatable for first iteration**/
            DataTable dataTable1 = processSelect(((SubSelect) fi).getSelectBody(),dataTable, tableName, level + 1,iterationNumber,orderBy,limit);
            createdTableName = generateTableName(tableName, level + 1);
            HashMap<String, DBschema> schema1 = dataTable1.getSchemaList().get(createdTableName).get(createdTableName);
            MyIterator iter1 = dataTable1.getMyIter().get(createdTableName);

            HashMap<String,ArrayList<PrimitiveValue>> col_map = new HashMap<>();
            col_map.put("", new ArrayList<>());
            HashMap<String,ArrayList<Aggregate>> agg_map = new HashMap<>();
            agg_map.put("", new ArrayList<>());
            List<ArrayList<PrimitiveValue>> data = new ArrayList<>();

            while (iter1.hasNext()) {
                try {
                    key = "";
                    rowValue = iter1.next();
                    populateMaps(key,rowValue,schema1,groupby,groups,agg_map,col_map,selectExpressionItemList,w);

                    /**datatable for next iteration**/
                    dataTable1 = processSelect(((SubSelect) fi).getSelectBody(),dataTable1, tableName, level + 1,iterationNumber,orderBy,limit);
                    iter1 = dataTable1.getMyIter().get(createdTableName);

                }catch(Exception e)
                {
                    break;
                }
            }

            /***populating datatypes***/
            ArrayList<PrimitiveValue> rowValue_eval = rowValue;
            Eval eval = new Eval() {
                @Override
                public PrimitiveValue eval(Column column) throws SQLException {

                    return rowValue_eval.get(schema1.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index);
                }
            };
            for(int i=0;i<selectExpressionItemList.size();i++)
            {
                Expression exp = selectExpressionItemList.get(i).getExpression();
                if(exp instanceof Function)
                {
                    if(((Function) exp).isAllColumns())
                        dataTypes.add("LONG");
                    else
                        dataTypes.add(eval.eval(((Function) exp).getParameters().getExpressions().get(0)).getType().toString());
                }
                else
                {
                    dataTypes.add(eval.eval(exp).getType().toString());
                }

            }

            /**adding the data to a stringbuilder list**/
            buildStringBuilder(groups,agg_map,col_map,selectExpressionItemList,data);

            /***check for empty result***/
            if(data.size()==0 && groupby.size()==0)
            {
                ArrayList<PrimitiveValue> data_line = new ArrayList<>();
                for(int i=0;i<selectExpressionItemList.size();i++)
                {
                    Expression exp = selectExpressionItemList.get(i).getExpression();
                    if(exp instanceof Function)
                    {
                        if(((Function) exp).isAllColumns())
                            data_line.add(new LongValue("0"));
                        else
                            data_line.add(new StringValue(""));
                    }
                    else
                        data_line.add(new StringValue(""));
                }
                data.add(data_line);
            }


            /** SETTING THE CURRENT LEVEL DATATABLE**/
            String current_createdTableName=generateTableName(tableName,level);
            getCreateQuery(current_createdTableName,selectExpressionItemList, dataTypes);
            dataTable1.getTableNames().put(level, current_createdTableName);

            /***********Setting schema**********/
            HashMap<String,HashMap<String,DBschema>> currentSchema;
            if((currentSchema=dataTable1.getSchemaList().get(current_createdTableName))==null){
                currentSchema=new HashMap<>();
            }
            currentSchema.put(current_createdTableName,schema.get(current_createdTableName));
            dataTable1.getSchemaList().put(current_createdTableName,currentSchema);

            /***********Setting data**********/
            dataTable1.getAllTableData().put(current_createdTableName, data);

            /***********Setting Iterator**********/
            MyIterator iter;
            iter=new MyIterator((ArrayList<ArrayList<PrimitiveValue>>)data);
            dataTable1.getMyIter().put(current_createdTableName, iter);

            /***********Setting TableName**********/
            dataTable1.getTableNames().put(level, current_createdTableName);

            return dataTable1;

        }else{

            DataTable dataTable1= processSelect(((SubSelect) fi).getSelectBody(),dataTable,tableName,level+1,iterationNumber,orderBy,limit);

            createdTableName=generateTableName(tableName, level+1);

            rowValue=dataTable1.getMyIter().get(createdTableName).next();

            return columnQuery(tableName, rowValue, selectExpressionItemList,level,dataTable1,w);
        }
    }

    /*** process a row from the input datatable and creates a datatable for passing to the next level ***/
    public static DataTable columnQuery(String t, ArrayList<PrimitiveValue> rowValue, List<SelectExpressionItem> selectExpressionItemList,int level,DataTable dataTable, Expression w) throws IOException, SQLException, ParseException, net.sf.jsqlparser.parser.ParseException {

        ArrayList<PrimitiveValue> sb = new ArrayList<>();
        String createdTableName=generateTableName(t,level);
        HashMap<String,DBschema> temp = dataTable.getSchemaList().get(generateTableName(t,level+1)).get(generateTableName(t,level+1));

        if(rowValue.size()==0 || rowValue==null){

            /***********Setting data**********/
            List<ArrayList<PrimitiveValue>> rows;
            rows=new ArrayList<>();
            rows.add(sb);

            dataTable.getAllTableData().put(createdTableName, rows);


            /***********Setting Iterator**********/
            MyIterator iter;

            iter=new MyIterator((ArrayList<ArrayList<PrimitiveValue>>)rows);

            dataTable.getMyIter().put(createdTableName, iter);

            /***********Setting TableName**********/

            dataTable.getTableNames().put(level, createdTableName);

            /****************Setting Schema**********/
            HashMap<String,HashMap<String,DBschema>> currentSchema;

            currentSchema=new HashMap<String,HashMap<String,DBschema>>();

            currentSchema.put(createdTableName,schema.get(createdTableName));

            dataTable.getSchemaList().put(createdTableName,currentSchema);

            return dataTable;
        }

        List<String> dataTypes=new ArrayList<String>();

        /*** define eval ***/
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                return rowValue.get(temp.get(column.getTable().getName()!=null ? column.getTable().getName()+"."+column.getColumnName() : column.getColumnName()).index);

            }
        };

        boolean isRowSelected=true;

        /*** check for where condition ***/
        if(w!=null)
        {
            PrimitiveValue result = eval.eval(w);
            if(result.toString().equalsIgnoreCase("FALSE")) {
                isRowSelected =false;
            }

        }

        if(isRowSelected){
            for(int i=0;i<selectExpressionItemList.size();i++) {

                Expression exp = selectExpressionItemList.get(i).getExpression();
                PrimitiveValue result = eval.eval(exp);
                dataTypes.add(result.getType().toString());
                sb.add(result);
            }
        }else{

            for(int i=0;i<selectExpressionItemList.size();i++) {

                Expression exp = selectExpressionItemList.get(i).getExpression();
                PrimitiveValue result = eval.eval(exp);
                dataTypes.add(result.getType().toString());
            }

        }


        getCreateQuery(createdTableName,selectExpressionItemList, dataTypes);

        /***********Setting schema**********/
        HashMap<String,HashMap<String,DBschema>> currentSchema;


        if((currentSchema=dataTable.getSchemaList().get(createdTableName))==null){

            currentSchema=new HashMap<String,HashMap<String,DBschema>>();

        }

        currentSchema.put(createdTableName,schema.get(createdTableName));
        dataTable.getSchemaList().put(createdTableName,currentSchema);



        /***********Setting data**********/
        List<ArrayList<PrimitiveValue>> rows = new ArrayList<>();

        rows.add(sb);

        dataTable.getAllTableData().put(createdTableName, rows);


        /***********Setting Iterator**********/
        MyIterator iter;

        iter=new MyIterator((ArrayList<ArrayList<PrimitiveValue>>)rows);

        dataTable.getMyIter().put(createdTableName, iter);

        /***********Setting TableName**********/

        dataTable.getTableNames().put(level, createdTableName);

        return dataTable;
    }

    /*** to create table query for maintaining DB schema at level in the Datatable ***/
    public static void getCreateQuery(String tableName,List<SelectExpressionItem> selectExpressionItemList, List<String> dataTypes){

        String columnDataType="",columnName="";

        HashMap<String,DBschema> currentSchema = new HashMap<>();
        for(int i=0;i<selectExpressionItemList.size();i++){

            SelectExpressionItem sei=selectExpressionItemList.get(i);

            columnName=getFieldName(sei.getAlias(),sei.getExpression(),tableName,i);

            columnDataType=columnDataType+" "+parserToQueryDataType.get(dataTypes.get(i));

            DBschema db = new DBschema(i,columnDataType);
            currentSchema.put(columnName,db);
        }

        schema.put(tableName,currentSchema);
    }

    /*** to create the column names for the DB schema ***/
    public static String getFieldName(String alias, Expression exp, String tableName, int columnNumber){

        String columnName = new String();
        if(exp instanceof Column)
        {
            columnName = ((Column)exp).getColumnName();
        }
        else
        {
            columnName = exp.toString();
        }
        if(alias!=null){

            return alias;

        }else{

            String pattern="^[^<>'\"/;`%+-//()^!@#~$&*=:?.{} ]*$";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(columnName);
            if(m.matches()){

                return ((Column)exp).getTable().getName()+"."+columnName;
            }else{

                return tableName+"_column"+columnNumber;
            }
        }
    }


    /*** to generate a unique table name to uniquely identify Datatable created at each level of the nested query ***/
    public static String generateTableName(String tableName, int level){

        return tableName+"_"+level;
    }

    /*** to display the final output from the Datatable created for the outermost level of the select clause ***/
    public static void displayDataTable(DataTable dataTable,int select){

        List<ArrayList<PrimitiveValue>> tableRows = dataTable.getAllTableData().get(dataTable.getTableNames().get(0));
        //need to set tablename to get(0)
        for(ArrayList<PrimitiveValue> row:tableRows){

            if(row.size()>0){

                for(int i=0;i<select;i++){
                    System.out.print(row.get(i).toRawString());

                    if(i<select-1)
                        System.out.print("|");
                }

                if(row.size()>0)
                    System.out.println("");

            }
        }
    }

    /*** to get the list of the expressions from the select clause ***/
    public static List<SelectExpressionItem> getSelectExpressionItemlist(SelectBody sb) throws SQLException{

        PlainSelect ps = (PlainSelect) sb;


        List<SelectItem> si = ps.getSelectItems();
        List<SelectExpressionItem> selectExpressionItemList = new ArrayList<>();

        for (int i = 0; i < si.size(); i++) {
            if (si.get(i) instanceof SelectExpressionItem) {
                SelectExpressionItem sei = (SelectExpressionItem) si.get(i);
                selectExpressionItemList.add(sei);

            } else
                throw new SQLException("Unidentified Select Expression");
        }

        return selectExpressionItemList;

    }

}