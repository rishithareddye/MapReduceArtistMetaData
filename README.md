Overview:
In our project the main aim is to learn and design the Big Data processing application and run it on Hadoop Map/Reduce environment using Java programming. In this project we use publicly available dataset from Million Song Database project.
Tasks and Observations:
Task 0: Merge first two metadata tables into a new Table0.
We use the tracks_per_year.txt and unique_artists.txt as the metadata for this task. The desired output for this task is in the form track_id<SEP>year<SEP>artist_id<SEP>artist_name. We do the following procedure for this task:
1.	Use track id as key value, as track id is present in both input files. I have used two mappers one for file one (tracks_per_year.txt) and the other for the unique_artist.txt
2.	In mapper one I have used *#* as identifier and *!* in the mapper two.
3.	We have to get year from tracks_per_year.txt and set them as value to key track id.
4.	We have to get artist id and artist name from unique_artists.txt and set them as value to key track id.
5.	Now these key value pairs from mapper classes are sent to reducer class. The reducer class gets all values corresponding to a key as input.
6.	In reducer class we need to get year, artist name and artist id from value and track id from key. The year from value can be obtained by using the logic that ends with *#*. Similarly we can get the value of artist id which ends with *!*. After getting artist id and year from values the remaining value is artist name. The output of reducer class will be of form track_id<SEP>year<SEP>artist_id<SEP>artist_name.
7.	After successful creation of output (part-r-00000), it is placed in the input folder with name table0.txt.
Task 1: Merge output of Task0 with third table into new Table1.
In this task we have to take metadata from Table0.txt and artists_location.txt. We have to generate output which is of the form artist_id<SEP>track_id<SEP>year<SEP>location. To perform this task we have to proceed as follows:
1.	Use artist id as key value, as track id is present in both input files. I have used two mappers one for file one (table0.txt) and the other for the artist_location.txt
2.	In mapper one I have used *#* as identifier and *!* in the mapper two.
3.	We have to get year and track id from table0.txt and set them as value to key artist id.
4.	We have to get location from artist_location.txt and set them as value to key artist id.
5.	Now these key value pairs from mapper classes are sent to reducer class. The reducer class gets all values corresponding to a key as input.
6.	In reducer class we need to get year, track id and location from value and artist id from key. The year from value can be obtained by using the logic that ends with *#*. Similarly we can get the value of location which ends with *!*. After getting track id and year from values the remaining value is location. The output of reducer class will be of form artist_id<SEP>track_id<SEP>year<SEP>location.

7.	After successful creation of output (part-r-00000), it is placed in the input folder with name table1.txt.

Task 2: i. Find number of tracks for each location and form a new Table2.
In this task we have to take metadata from Table1.txt and we need to calculate number of tracks per location. We have to generate output which is of the form location<SEP># tracks per location. To perform this task we have to proceed as follows:
1.	Use location as key value. In mapper class we need to do following point.
2.	We have to get track id from Table1.txt and set it as value to key location.
3.	Now these key value pairs from mapper class are sent to reducer class. The reducer class gets all values corresponding to a key as input. In reducer class we need to get number of tracks from values and location from key. Number of tracks are calculated by iterating a loop until it reaches end of values for a give key (location) and by calculating the number of loop iterations we get number of tracks per location. The output of the reducer class will be of the form location<SEP># of tracks per location.

4.	After successful creation of output (part-r-00000), it is placed in the input folder with name table2.txt.

Task 2: ii. Find number of tracks for each year and form a new Table3.
In this task we have to take metadata from Table1.txt and we need to calculate number of tracks per year. We have to generate output which is of the form year<SEP># tracks per year. To perform this task we have to proceed as follows:
1.	Use year as key value. In mapper class we need to do following point.
2.	We have to get track id from Table1.txt and set it as value to key year.
3.	Now these key value pairs from mapper class are sent to reducer class. The reducer class gets all values corresponding to a key as input. In reducer class we need to get number of tracks from values and year from key. Number of tracks are calculated by iterating a loop until it reaches end of values for a give key (year) and by calculating the number of loop iterations we get number of tracks per year. The output of the reducer class will be of the form year<SEP># of tracks per year.

4.	After successful creation of output (part-r-00000), it is placed in the input folder with name table3.txt.




Observations:
Some records does not have all values. So we have to filter those kind of records. Some values have unnecessary spaces, so we need to trim them. Mapper class and reducer class both work on key value pairs. Hadoop uses Text object instead of String and IntWritable object instead of int and so on, because while working on very huge data it takes more time when we use normal int and string. To work more efficiently on huge data, to take little amount space to store, to take small amount of time to move data and to provide serialization of data Hadoop uses Text, IntWritable and soon. So key value pair must always be in objects (Text) instead of normal primitive data type. So we need to manually type cast between object data type and primitive data types.
