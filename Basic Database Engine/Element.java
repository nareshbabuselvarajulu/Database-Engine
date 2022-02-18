import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;

import java.sql.SQLException;

/**
 * Created by shruti on 20/4/17.
 */
class Element{
    PrimitiveValue value;
    int index;

    public Element(PrimitiveValue value,int i)
    {
        this.value = value;
        this.index = i;
    }

//    @Override
    public int compareTo(PrimitiveValue o) throws SQLException {
        Eval eval = new Eval() {
            @Override
            public PrimitiveValue eval(Column column) throws SQLException {
                return null;
            }
        };

        if(o.getType().toString().equalsIgnoreCase("STRING"))
        {
            String s1 = this.value.toRawString();
            String s2 = o.toRawString();
            return s1.compareTo(s2);
        }

        //check for equals to
        EqualsTo e = new EqualsTo(this.value,o);
        PrimitiveValue equals = eval.cmp(e,new Eval.CmpOp() {
            public boolean op(long a, long b){ return a == b; }
            public boolean op(double a, double b){ return a == b; }
            public String toString() { return "=="; }
        });
        if(equals.toBool())
            return 0;

        GreaterThan g = new GreaterThan(this.value,o);
        PrimitiveValue greater = eval.cmp(g,new Eval.CmpOp() {
            public boolean op(long a, long b){ return a > b; }
            public boolean op(double a, double b){ return a > b; }
            public String toString() { return ">"; }
        });
        if(greater.toBool())
            return 1;
        else
            return -1;
    }

}
