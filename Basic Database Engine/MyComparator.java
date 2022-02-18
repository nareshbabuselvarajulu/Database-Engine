//package dubstep;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by shruti on 15/4/17.
 */
public class MyComparator implements Comparator<ArrayList<PrimitiveValue>> {

    HashMap<String, DBschema> schema;
    List<OrderByElement> orderBy;

    public MyComparator(HashMap<String, DBschema> schema2, List<OrderByElement> orderBy2)
    {
        this.schema = schema2;
        this.orderBy = orderBy2;
    }


    @Override
    public int compare(ArrayList<PrimitiveValue> o1, ArrayList<PrimitiveValue> o2) {

//        String[] s1 = o1.toString().split("\\|");
//        String[] s2 = o2.toString().split("\\|");

//        if (Integer.parseInt(s1[s1.length - 1]) >= orderBy.size())
//            return 0;


//                int compare = s1[orderBy.get(Integer.parseInt(s1[s1.length-1]))].compareTo(s2[orderBy.get(Integer.parseInt(s1[s1.length-1]))]);

        /***check***/
//                            String fieldName = getFieldName(((Column)orderBy.get(Integer.parseInt(s1[s1.length - 1])).getExpression()).ge)
        /***check***/
        int index1 = schema.get(((Column) orderBy.get(1).getExpression()).getColumnName()).index;
        int index2 = schema.get(((Column) orderBy.get(1).getExpression()).getColumnName()).index;
//        String d1 = schema.get(((Column) orderBy.get(1).getExpression()).getColumnName()).datatype;
//        String d2 = schema.get(((Column) orderBy.get(1).getExpression()).getColumnName()).datatype;

//        if (d1.equals("int") || d1.equals("double")) {
////            if (Double.parseDouble(s1[index1]) == Double.parseDouble(s2[index2])) {
////                int compare1 = compare(o1.append("|" + Integer.toString(Integer.parseInt(s1[s1.length - 1]) + 1)), o2.append("|" + Integer.toString(Integer.parseInt(s2[s2.length - 1]) + 1)));
////                o1.delete(o1.toString().length() - 2, o1.toString().length());
////                o2.delete(o2.toString().length() - 2, o2.toString().length());
////                return compare1;
////            } else {
//
//            if (Double.parseDouble(s1[index1]) > Double.parseDouble(s2[index2]))
//            {
//                if(orderBy.get(Integer.parseInt(s1[s1.length - 1])).isAsc())
//                    return 1;
//                return -1;
//            }
//            else {
//                if(orderBy.get(Integer.parseInt(s1[s1.length - 1])).isAsc())
//                    return -1;
//                return 1;
//            }
////            }
//        }


//        if (s1[index1].compareTo(s2[index2]) == 0) {
////                    orderBy.remove(0);
//            int compare1 = compare(o1.append("|" + Integer.toString(Integer.parseInt(s1[s1.length - 1]) + 1)), o2.append("|" + Integer.toString(Integer.parseInt(s2[s2.length - 1]) + 1)));
//            o1.delete(o1.toString().length() - 2, o1.toString().length());
//            o2.delete(o2.toString().length() - 2, o2.toString().length());
//            return compare1;
//        } else {
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                return null;
            }
        };

        //check for equals to
        int result=0;
        EqualsTo e = new EqualsTo(o1.get(index1),o2.get(index2));
        PrimitiveValue equals = null;
        try {
            equals = eval.cmp(e,new Eval.CmpOp() {
                public boolean op(long a, long b){ return a == b; }
                public boolean op(double a, double b){ return a == b; }
                public boolean op(String a, String b){
                    if(a.compareTo(b)==0)
                        return true;
                    return false;
                }
                public String toString() { return "=="; }
            });
            if(equals.toBool())
                result = 0;



        GreaterThan g = new GreaterThan(o1.get(index1),o2.get(index2));
        PrimitiveValue greater = eval.cmp(g,new Eval.CmpOp() {
            public boolean op(long a, long b){ return a > b; }
            public boolean op(String a, String b){
                if(a.compareTo(b)>0)
                    return true;
                return false;
            }
            public boolean op(double a, double b){ return a > b; }
            public String toString() { return ">"; }
        });
        if(greater.toBool())
            result = 1;
        else
            result = -1;
        } catch (SQLException e1) {
            e1.printStackTrace();
        }



//        int result = o1.get(index1).compareTo(o2.get(index2));
        if(orderBy.get(1).isAsc())
            return result;
        return -1*result;
//        }
    }
}