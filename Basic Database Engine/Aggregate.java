//package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;

/**
 * Created by shruti on 11/4/17.
 */
class Aggregate{

    private AggregateResult result;

    private long rowCount;

    private Object total;

    public Aggregate(){

        result=new AggregateResult();

        rowCount=0;

        total=null;
    }


    public void doFunction(String functionName,PrimitiveValue pValue) throws PrimitiveValue.InvalidPrimitive {

        if(functionName.equalsIgnoreCase("count")){
            rowCount++;
            result.setValue(Long.valueOf(rowCount),"LONG");
            return;
        }

        if(pValue.getType().toString().equalsIgnoreCase("STRING")){

            String value=pValue.toRawString();

            if(functionName.equalsIgnoreCase("max"))
                max(value);
            else if(functionName.equalsIgnoreCase("min"))
                min(value);
            else{
                throw new PrimitiveValue.InvalidPrimitive();
            }
        }

        else if(pValue.getType().toString().equalsIgnoreCase("LONG")){

            Long value=pValue.toLong();

            if(functionName.equalsIgnoreCase("max"))
                max(value);
            else if(functionName.equalsIgnoreCase("sum"))
                sum(value);
            else if(functionName.equalsIgnoreCase("min"))
                min(value);
            else if(functionName.equalsIgnoreCase("avg"))
                avg(value);

        }
        else if(pValue.getType().toString().equalsIgnoreCase("DOUBLE")){

            Double value=pValue.toDouble();

            if(functionName.equalsIgnoreCase("max"))
                max(value);
            else if(functionName.equalsIgnoreCase("sum"))
                sum(value);
            else if(functionName.equalsIgnoreCase("min"))
                min(value);
            else if(functionName.equalsIgnoreCase("avg"))
                avg(value);

        }
//        else{
//
//            throw new PrimitiveValue.InvalidPrimitive();
//        }

        return;

    }

    private void avg(Long val1) {
        if(result.getValue()==null){
            result.setValue(Long.valueOf(0),"LONG");
        }
        if(total==null){
            total=Long.valueOf(0);
        }
        total=  (Long)total + val1;
        rowCount++;
        result.setValue((Long)total/rowCount,"LONG");
        return;
    }

    private void avg(Double val1) {
        if(result.getValue()==null){
            result.setValue(Double.valueOf(0.0),"DOUBLE");
        }
        if(total==null){
            total=Double.valueOf(0.0);
        }
        total=  (Double)total + val1;
        rowCount++;
        result.setValue((Double)total/rowCount,"DOUBLE");
        return;
    }


    public void min(String val1){
        if(result.getValue()==null){
            result.setValue(val1,"STRING");
        }
        if(((String)result.getValue()).compareTo(val1)>0){
            result.setValue(val1,"STRING");
        }
        return;
    }
    public void min(Double val1){
        if(result.getValue()==null){
            result.setValue(val1,"DOUBLE");
        }
        if(((Double)result.getValue())>val1){
            result.setValue(val1,"DOUBLE");
        }
    }
    public void min(Long val1){
        if(result.getValue()==null){
            result.setValue(val1,"LONG");
        }
        if(((Long)result.getValue())>val1){
            result.setValue(val1,"LONG");
        }
        return;
    }

    public void max(String val1){
        if(result.getValue()==null){
            result.setValue(val1,"STRING");
        }
        if(((String)result.getValue()).compareTo(val1)<0){
            result.setValue(val1,"STRING");
        }
        return;
    }
    public void max(Double val1){
        if(result.getValue()==null){
            result.setValue(val1,"DOUBLE");
        }
        if(((Double)result.getValue())<val1){
            result.setValue(val1,"DOUBLE");
        }
        return;
    }
    public void max(Long val1){
        if(result.getValue()==null){
            result.setValue(val1,"LONG");
        }
        if(((Long)result.getValue())<val1){
            result.setValue(val1,"LONG");
        }
        return;
    }

    public void sum(Double val1)
    {
        if(result.getValue()==null){
            result.setValue(Double.valueOf(0.0),"DOUBLE");
        }
        result.setValue((Double)result.getValue() + val1,"DOUBLE");
        return;
    }

    public void sum(Long val1)
    {
        if(result.getValue()==null){
            result.setValue(Long.valueOf(0),"LONG");
        }
        result.setValue((Long)result.getValue() + val1,"LONG");
    }

    public AggregateResult getResult(){

        return result;
    }
}