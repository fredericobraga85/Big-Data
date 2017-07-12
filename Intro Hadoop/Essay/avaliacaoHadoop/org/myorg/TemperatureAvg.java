package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

public class TemperatureAvg extends Configured implements Tool {


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new TemperatureAvg(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "temperatureAvg");
    job.setJarByClass(this.getClass());
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    
    return job.waitForCompletion(true) ? 0 : 1;
  }

public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

   public static final int MISSING = 9999;
           
   public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException
   {

	String line = value.toString();
	String year = line.substring(15,19);               
	int temperature;                    
      
	if (line.charAt(87)=='+')
		temperature = Integer.parseInt(line.substring(88, 92));
        else
		temperature = Integer.parseInt(line.substring(87, 92));       
                       
        String quality = line.substring(92, 93);
        
	if(temperature != MISSING && quality.matches("[01459]"))
		context.write(new Text(year),new IntWritable(temperature)); 
  }
}

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text key,  Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
  {          
	int max_temp = 0; 
        int count = 0;
        
	for (IntWritable value : values)
        {
            max_temp += value.get();     
            count+=1;
        }
            
	context.write(key, new IntWritable(max_temp/count));
  }      


}
}
