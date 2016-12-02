import java.io.IOException;
import java.util.Map;

import org.apache.pig.FilterFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.builtin.*;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.text.ParseException;

/**
 * Pig UDF .
 * Determine Whether record is include or not 
 */
public class checkDateRange extends FilterFunc {

    @Override
    public Boolean exec(Tuple input) throws IOException {
          SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        try {
	           String  manage_from =  (String)input.get(0);
	           String   manage_to =  (String)input.get(1);
	           String  emp_from =  (String)input.get(2);
	           String   emp_to =  (String)input.get(3);
	            Date manager_from_date = formatter.parse(manage_from.replaceAll("Z$", "+0000"));
	            Date manager_to_date = formatter.parse(manage_to.replaceAll("Z$", "+0000"));
	            Date emp_from_date = formatter.parse(emp_from.replaceAll("Z$", "+0000"));
	            Date emp_to_date = formatter.parse(emp_to.replaceAll("Z$", "+0000"));

	           // check whether employee works under manager or not 
	           // if emp join and last date between manager period 
	           if ((emp_from_date.compareTo(manager_from_date) > 0) && emp_from_date.compareTo(manager_to_date) < 0) {
	               return true;
	           }
	           else if(emp_from_date.compareTo(manager_from_date) < 0 && emp_to_date.compareTo(manager_from_date) > 0 ){
	               return true;
	           }
	        // if emp join and last date before manager period 
	           else if(emp_from_date.compareTo(manager_from_date) > 0  && emp_to_date.compareTo(manager_to_date) > 0) {
	               return true;
	           }
          
        } catch (ParseException e) {
            e.printStackTrace();
        }
  return false;              
}            
} 
