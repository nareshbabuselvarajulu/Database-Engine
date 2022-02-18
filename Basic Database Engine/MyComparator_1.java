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
public class MyComparator_1 implements Comparator<StringBuilder> {

    HashMap<String,DBschema> schema;
    List<OrderByElement> orderBy = new ArrayList<>();
    int orderBy_index;

    public MyComparator_1(HashMap<String,DBschema> schema, List<OrderByElement> o)
    {
        this.schema = schema;
        this.orderBy = o;
        this.orderBy_index = 0;

    }


    @Override
    public int compare(StringBuilder o1, StringBuilder o2) {

        String[] s1 = o1.toString().split("\\|");
        String[] s2 = o2.toString().split("\\|");
        if (orderBy_index >= orderBy.size()-1)
            return 0;
        int index = schema.get(((Column)orderBy.get(orderBy_index).getExpression()).getTable().getName()+"."+((Column)orderBy.get(orderBy_index).getExpression()).getColumnName()).index;
        if (s1[index].compareTo(s2[index]) == 0) {
            orderBy_index++;
            int compare1 = compare(o1, o2);
            orderBy_index--;
            return compare1;
        } else {
            return s1[index].compareTo(s2[index]);
        }
    }

}