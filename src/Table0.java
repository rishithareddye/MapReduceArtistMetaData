
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

public class Table0
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
            final String trackid = words[1].trim(); // storing track id
            final String year = words[0].trim(); // storing year

            con.write(new Text(trackid.trim()), new Text(year.trim() + "*#*")); // sending <key,value> pair to reducer with a identifier *#*
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
         if (size == 4)// checking with the length
         {
            final String trackid = words[2].trim();// storing track id
            final String artistid = words[0].trim();// storing artist id
            final String artistname = words[3].trim();// storing artistname
            con.write(new Text(trackid.trim()), new Text(artistid.trim() + "<SEP>" + artistname.trim() + "*!*"));// sending <key,value> pair to reducer with a identifier *!*
         }

      }
   }

   public static class Red extends Reducer<Text, Text, Text, Text>
   {

      @Override
      public void reduce(final Text key, final Iterable<Text> Values, final Context con)
            throws IOException, InterruptedException
      {
         final String trackid = key.toString().trim(); 
         String year = "";
         String artistid = "";
         String artistname = "";
         for (final Text val : Values)
         {
            final String temp = val.toString();
            if (temp.endsWith("*#*")) // identifying the file one
            {
               final StringTokenizer st = new StringTokenizer(val.toString(), "*#*"); // dividing the file to tokens
               while (st.hasMoreTokens())
               {
                  year = year + st.nextToken().trim(); // storing the year into variable
               }
            }
            if (temp.endsWith("*!*"))// identifying the second one
            {
               final StringTokenizer st = new StringTokenizer(val.toString(), "*!*"); // dividing the file to tokens
               while (st.hasMoreTokens())
               {
                  final String temp2 = st.nextToken().trim();
                  final String[] word = temp2.split("<SEP>"); // dividing the value with <SEP>
                  if (word.length == 2)//checking the length
                  {
                     artistid = artistid + word[0]; // storing the artistid
                     artistname = artistname + word[1]; // storing the artistname

                  }

               }
            }

         }
         if (!year.equals("") && !artistid.equals("") && !artistname.equals("")) // checking if all the values filled
         {
            con.write(new Text(
                  trackid.trim() + "<SEP>" + year.trim() + "<SEP>" + artistid.trim() + "<SEP>" + artistname.trim()),
                  null); // writing the key value to file
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
      c.set("mapreduce.textoutputformat.separator", "");
      final String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
      final Path p1 = new Path("input/tracks_per_year.txt"); // file one
      final Path p2 = new Path("input/unique_artists.txt"); // file two
      final Path p3 = new Path(files[0]);
      final Job j = new Job(c, "Table0");
      j.setJarByClass(Table0.class);
      j.setMapperClass(Map1.class); // mapper one
      j.setMapperClass(Map2.class); // mapper two
      j.setReducerClass(Red.class); // reducer
      j.setOutputKeyClass(Text.class);
      j.setOutputValueClass(Text.class);
      MultipleInputs.addInputPath(j, p1, TextInputFormat.class, Map1.class); // input to mapper1
      MultipleInputs.addInputPath(j, p2, TextInputFormat.class, Map2.class);// input to mapper2
      FileOutputFormat.setOutputPath(j, p3);// output file
      if(j.waitForCompletion(true)) // if the job is succesfull
      {
    	  copyFile(new File(files[0]+"/part-r-00000"),new File("input/Table0.txt")); // copy file part-r-00000 to Table0.txt for future use
    	  System.exit(0);
      }
      else
    	  System.exit(1);
      

   }

}
