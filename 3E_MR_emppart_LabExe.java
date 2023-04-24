
package org.map.emp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class emppart {

    public static class MapEmployee extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split(",");


            Text tx = new Text(elements[2]);

            int i = Integer.parseInt(elements[4]);
            IntWritable it = new IntWritable(i);
            context.write(tx, it);
        }
    }
    
    public static class DesignationPartitioner<K,V> extends Partitioner<K,V>{
    	
    	public int getPartition(K key,V value,int numReduceTasks){
    		
    		if(key.toString().equalsIgnoreCase("Manager"))
    			return 0;
    		else if(key.toString().equalsIgnoreCase("Developer"))
    			return 1;
    		else
    		  	return 2;
    	}
    	   	
    }

    public static class ReduceEmployee extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        
       
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }        

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Insufficient args");
           System.exit(-1);
        }
        

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Employee Records-Partitioner");

        job.setJarByClass(emppart.class); 
        

        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class);
        

        job.setMapperClass(MapEmployee.class);
        
        
        job.setReducerClass(ReduceEmployee.class);
 	   
        job.setNumReduceTasks(3);
        job.setPartitionerClass(DesignationPartitioner.class);
        
        
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setOutputFormatClass(TextOutputFormat.class);
        

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}

