

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class bfsianlz {
	
	    public static class Mapbfsi extends
            Mapper<LongWritable, Text, Text, IntWritable> {

       
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] vfld_names = line.split(",");

          //  String concatData = elements[1]+"#"+element
          //  s[2];
            
          //  Text word = new Text(concatData);
            
            Text word = new Text(vfld_names[2]);
            int valout = Integer.parseInt(vfld_names[4]);
          
            IntWritable intout = new IntWritable(valout);
            context.write(word, intout);
        }
    }
    
    public static class Reducebfsi extends
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
            System.err.println("Plz check again");
           System.exit(-1);
        } 
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"BFSI Data");

        

        job.setJarByClass(bfsianlz.class); 

        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class); 
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(Mapbfsi.class);
        job.setReducerClass(Reducebfsi.class);
 	
	job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }

}




