import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class EmpMovement {

	/**
	 * Mapper class 
	 * Here mapper is sending key value as it is to reducer 
	 * example input to mapper (499998,"Patricia,Breugel,Senior Staff,Finance") here Key will be 499998 and remaining string is value 
	 */
	
  public static class EmpTitleMapper
       extends Mapper<Text, Text, Text,Text>{

    private Text result = new Text();

    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
      List<String> s = new ArrayList<String>();
  	  String listString = "";
  	  s.add(value.toString());
  	  listString = StringUtils.join(s,',');
      result.set(listString);
      context.write(key, result);  // send as it is string to reducer
    }
  }

  /**
   * 
   * Reducer class;
   * example input to reducer (499998,["Patricia,Breugel, Staff,Finance" "Patricia,Breugel,Senior Staff,Finance"]) here Key will be 499998 and remaining string is value 
   */
  public static class EmpTitleReducer
       extends Reducer<Text,Text,Text,Text> {  // input key type Text, value Text, Output key type Text, vlaue Text 
	  private Text result = new Text();
	  private Text new_key = new Text("\n");

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	List<String> s = new ArrayList<String>();
    	String listString = "";
    	List<String> name = new ArrayList<String>();
      for (Text val : values) {  // We will get all the records asociated with emp_no here . emp_no will be kay and all records as value string.
    	      String input = "";
    		  input = val.toString();
    		  int j = 0;
    		  name.clear();
    		  for (String retval: input.split(",")) { // split individual record by comma
    		         if(j==0 || j== 1) {  // Check for first name and last name.
    		        	 name.add(retval); // we are adding names only once in the output record. so it will overwrite previous values 
    		         }
    		         else {
    		        	 s.add(retval.toString()); 
    		         }
    		         j++;
    		  }
      }
      listString = ',' +StringUtils.join(name,',') + ',' + StringUtils.join(s,',');  // concat string first name and remaining string 
      result.set(listString);
    
      context.write(key, result); // Write result to output file 
    }
  }

 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ","); // set key value seperator 
    Job job = Job.getInstance(conf, "Employee movement");
    job.setInputFormatClass(KeyValueTextInputFormat.class);  // set input file type KeyValueTextInputFormat we are assuming that key will be emp_no 
    job.setJarByClass(EmpMovement.class);
    job.setMapperClass(EmpTitleMapper.class);  // set mapper class 
    job.setCombinerClass(EmpTitleReducer.class); 
    job.setReducerClass(EmpTitleReducer.class);  // set reducer class 
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
