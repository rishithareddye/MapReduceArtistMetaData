

/**
 *
 */
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Table1
{
   public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
   {
     
      @Override
      public void map(final LongWritable key, final Text Values, final Context con)
            throws IOException, InterruptedException
      {

         final String line = Values.toString();
         final String[] words = line.split("<SEP>"); // splitting the line using <SEP> as separator
         final int size = words.length;
         if (size == 4) // checking with the length
         {
            final String artistid = words[2].trim(); // storing artist id
            final String trackid = words[0].trim(); // storing track id
            final String year = words[1].trim();// storing year

            // System.out.println(words[3]);
            con.write(new Text(artistid.trim()), new Text(trackid.trim() + "<SEP>" + year.trim() + "*#*")); // sending <key,value> pair to reducer with a identifier *#*
         }
         
      }
   }

   public static class Map2 extends Mapper<LongWritable, Text, Text, Text>
   {
      @Override
      public void map(final LongWritable key, final Text Values, final Context con)
            throws IOException, InterruptedException
      {
         

         final String line = Values.toString();
         final String[] words = line.split("<SEP>"); // splitting the line using <SEP> as separator
         final int size = words.length;
         if (size == 5)// checking with the length
         {
            final String artistid = words[0].trim();// storing artist id
            final String location = words[4].trim();// storing location
            
            con.write(new Text(artistid.trim()), new Text(location.trim() + "*!*")); // sending <key,value> pair to reducer with a identifier *!*
         }

      }
   }

   public static class Red extends Reducer<Text, Text, Text, Text>
   {

     
      @Override
      public void reduce(final Text key, final Iterable<Text> Values, final Context con)
            throws IOException, InterruptedException
      {
         final String artistid = key.toString().trim();
         String year = "";
         String trackid = "";
         String location = "";
         
         for (final Text val : Values)
         {
            final String temp = val.toString();
            if (temp.endsWith("*!*")) // identifying the file one
            {
               final StringTokenizer st = new StringTokenizer(val.toString(), "*#*"); // dividing the file to tokens
               while (st.hasMoreTokens())
               {
                  location = location + st.nextToken().trim();// storing the location into variable
               }
            }
            if (temp.endsWith("*#*"))// identifying the second one
            {
               final StringTokenizer st = new StringTokenizer(val.toString(), "*!*");// dividing the file to tokens
               while (st.hasMoreTokens())
               {
                  final String temp2 = st.nextToken().trim();
                  final String[] word = temp2.split("<SEP>");// dividing the value with <SEP>
                  if (word.length == 2)//checking the length
                  {
                     trackid = trackid + word[0];// storing the trackid
                     year = year + word[1];// storing the year

                  }

               }
            }

         }
         if (!year.equals("") && !trackid.equals("") && !location.equals(""))// checking if all the values filled
         {
            con.write(
                  new Text(
                        artistid.trim() + "<SEP>" + trackid.trim() + "<SEP>" + year.trim() + "<SEP>" + location.trim()),
                  null);// writing the key value to file
         }

      }
   }

   static
   {
      final Logger rootLogger = Logger.getRootLogger();
      rootLogger.setLevel(Level.INFO);
      rootLogger.addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));

   }
   private static void copyFile(File source, File dest)
			throws IOException {
		FileUtils.copyFile(source, dest);
	}

   @SuppressWarnings("deprecation")
   public static void main(final String[] args) throws Exception
   {
      final Configuration c = new Configuration();
      final String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
      final Path p1 = new Path("input/Table0.txt");// file one
      final Path p2 = new Path("input/artist_location.txt");// file two
      final Path p3 = new Path(files[0]);
      final Job j = new Job(c, "Table1");
      j.setJarByClass(Table1.class);
      j.setMapperClass(Map1.class);// mapper one
      j.setMapperClass(Map2.class);// mapper two
      j.setReducerClass(Red.class);// reducer
      j.setOutputKeyClass(Text.class);
      j.setOutputValueClass(Text.class);
      c.set("mapreduce.textoutputformat.separator", "");
      MultipleInputs.addInputPath(j, p1, TextInputFormat.class, Map1.class);// input to mapper1
      MultipleInputs.addInputPath(j, p2, TextInputFormat.class, Map2.class);// input to mapper2
      FileOutputFormat.setOutputPath(j, p3);// output file
      if(j.waitForCompletion(true))// if the job is succesfull
      {
    	  copyFile(new File(files[0]+"/part-r-00000"),new File("input/Table1.txt"));// copy file part-r-00000 to Table1.txt for future use
    	  System.exit(0);
      }
      else
    	  System.exit(1);

   }

}
