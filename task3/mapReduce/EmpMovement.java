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

  public static class TokenizerMapper
       extends Mapper<Text, Text, Text,Text>{

    private Text result = new Text();

    public void map(Text key, Text value, Context context
                    ) throws IOException, InterruptedException {
      List<String> s = new ArrayList<String>();
  	String listString = "";
  	s.add(value.toString());

  	listString = StringUtils.join(s,',');
    result.set(listString);
      context.write(key, result);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
//    private IntWritable result = new IntWritable();
	  private Text result = new Text();
	  private Text new_key = new Text("\n");

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	List<String> s = new ArrayList<String>();
    	
    	String listString = "";
    	int i = 0;
    	List<String> name = new ArrayList<String>();
      for (Text val : values) {
//    	  if(i == 0) {
    	  String input = "";
    		  input = val.toString();
    		  int j = 0;
    		  name.clear();
    		  for (String retval: input.split(",")) {
    		         if(j==0 || j== 1) {
    		        	 name.add(retval);
    		         }
    		         else {
    		        	 s.add(retval.toString());
    		         }
    		         j++;
    		      }
//    	  }
    	  
//    	  i++;
      }
      listString = ',' +StringUtils.join(name,',') + ',' + StringUtils.join(s,',');
      result.set(listString);
    
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
    Job job = Job.getInstance(conf, "Employee movement");
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setJarByClass(EmpMovement.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
