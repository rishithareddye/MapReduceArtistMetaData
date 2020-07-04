/**
 *
 */
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Table3
{
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
   {
      @Override
      public void map(final LongWritable key, final Text Values, final Context con)
            throws IOException, InterruptedException
      {

         final String line = Values.toString();//converting the value to string
         final String[] words = line.split("<SEP>");//splitting the value using separator
         final int size = words.length;
         if (size == 4)// checking the length
         {
            final String year = words[2].trim();//storing the year

            con.write(new Text(year), new IntWritable(1));//sending <key,1> to reducer(wordcount logic)
         }
        }
   }

   public static class Red extends Reducer<Text, IntWritable, Text, IntWritable>
   {

      @Override
      public void reduce(final Text key, final Iterable<IntWritable> Values, final Context con)
            throws IOException, InterruptedException
      {
         final String year = key.toString().trim();// storing year(key)
         int sum = 0;
         for (final IntWritable val : Values)// for every unique location sum=sum+1
         {
            sum += val.get();//sum=sum+1
         }
         con.write(new Text(year.trim() + "<SEP>"+sum), null);//writing key<SEP>sum(number of repetition)
      }
   }

   static
   {
      final Logger rootLogger = Logger.getRootLogger();
      rootLogger.setLevel(Level.INFO);
      rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));

   }
   private static void copyFile(File source, File dest)//moving file to desired location
			throws IOException {
		FileUtils.copyFile(source, dest);//using file util to transfer
	}
   public static void main(final String[] args) throws Exception
   {
      final Configuration conf = new Configuration();
      final Job job = Job.getInstance(conf, "Table2");
      job.setJarByClass(Table3.class);
      job.setMapperClass(Map.class);// mapper class
      job.setReducerClass(Red.class);// reducer class
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path("input/Table1.txt"));// input to mapper
      FileOutputFormat.setOutputPath(job, new Path(args[0]));//output file name
      if(job.waitForCompletion(true))// if the job is succesfull
      {
    	  copyFile(new File(args[0]+"/part-r-00000"),new File("input/Table3.txt"));// copy file part-r-00000 to Table3.txt for future use
    	  System.exit(0);
      }
      else
    	  System.exit(1);
   }
}
