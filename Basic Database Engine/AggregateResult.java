//package dubstep;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;

/**
 * Created by shruti on 15/4/17.
 */
class AggregateResult{

    String valueString;
    String type;
    Object value;

    public AggregateResult(){

        valueString=null;
        type=null;
        value=null;
    }

    public PrimitiveValue getPrimitiveValue()
    {
        if(this.type.equalsIgnoreCase("LONG"))
            return new LongValue(this.value.toString());
        else if(this.type.equalsIgnoreCase("DOUBLE"))
            return new DoubleValue(this.value.toString());
        else
            return new StringValue((this.value.toString()));
    }

    public String getValueString() {
        return valueString;
    }
    public void setValueString(String valueString) {
        this.valueString = valueString;
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public Object getValue() {
        return value;
    }
    public void setValue(Object value, String type) {
        this.value = value;
        setType(type);
    }
}