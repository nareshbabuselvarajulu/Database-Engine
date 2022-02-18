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
public class MyComparator_2 implements Comparator<ArrayList<PrimitiveValue>> {

    HashMap<String,DBschema> schema;
    List<OrderByElement> orderBy = new ArrayList<>();
    int orderBy_index;

    public MyComparator_2(HashMap<String,DBschema> schema, List<OrderByElement> o)
    {
        this.schema = schema;
        this.orderBy = o;
        this.orderBy_index = 0;

    }


    @Override
    public int compare(ArrayList<PrimitiveValue> o1, ArrayList<PrimitiveValue> o2) {

        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                return null;
            }
        };

        //check for equals to
        int result=0;
        int index = schema.get(((Column)orderBy.get(orderBy_index).getExpression()).getTable().getName()!=null? ((Column)orderBy.get(orderBy_index).getExpression()).getTable().getName()+"."+((Column)orderBy.get(orderBy_index).getExpression()).getColumnName() : ((Column)orderBy.get(orderBy_index).getExpression()).getColumnName()).index;

        if(o1.get(index).getType().toString().equalsIgnoreCase("STRING"))
        {
            if(o1.get(index).toRawString().compareTo(o2.get(index).toRawString())==0)
            {
                if(orderBy_index>=orderBy.size()-1)
                    result = 0;
                else {
                    orderBy_index = orderBy_index+1;
                    result = compare(o1, o2);
                    orderBy_index = orderBy_index-1;
                }
            }

            else
                result = o1.get(index).toRawString().compareTo(o2.get(index).toRawString());
        }


        else {
            EqualsTo e = new EqualsTo(o1.get(index), o2.get(index));
            PrimitiveValue equals = null;
            try {
                equals = eval.cmp(e, new Eval.CmpOp() {
                    public boolean op(long a, long b) {
                        return a == b;
                    }

                    public boolean op(double a, double b) {
                        return a == b;
                    }
                });
                if (equals.toBool()) {
                    if (orderBy_index >= orderBy.size()-1)
                        result = 0;
                    else {
                        orderBy_index = orderBy_index + 1;
                        result = compare(o1, o2);
                        orderBy_index = orderBy_index - 1;
                    }
                } else {
                    GreaterThan g = new GreaterThan(o1.get(index), o2.get(index));
                    PrimitiveValue greater = eval.cmp(g, new Eval.CmpOp() {
                        public boolean op(long a, long b) {
                            return a > b;
                        }

                        public boolean op(double a, double b) {
                            return a > b;
                        }
                    });
                    if (greater.toBool())
                        result = 1;
                    else
                        result = -1;
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }

        if(orderBy.get(orderBy_index).isAsc())
            return result;
        return -1*result;
    }
}