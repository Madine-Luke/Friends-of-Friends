import java.io.IOException;
import java.net.Inet4Address;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FriendsOfFriends {

  public static class FriendMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

	  int mark_red = -1;
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		  // "1 2"
	  StringTokenizer itr = new StringTokenizer(value.toString());
	  String one_string = itr.nextToken(); //"1"
	  String two_string = itr.nextToken(); //"2"
	  int one_integer = Integer.parseInt(one_string);
	  int two_integer = Integer.parseInt(two_string);

	  //TODO
		  //R1
		  //emit(1,2)
		  //    (2,3)
		  //    (2,4)
		  context.write(new IntWritable(one_integer), new IntWritable(two_integer));
		  //R2
		  //emit(2,1(red))
		  //	(3,2(red))
		  //	(4,2(red))
		  context.write(new IntWritable(two_integer), new IntWritable(one_integer * mark_red));
      }
  }

  public static class FriendReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

      private int mark_red = -1;

      public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	  Set<Integer> srcs = new HashSet<Integer>();
	  Set<Integer> dest = new HashSet<Integer>();

	  //TODO
		  // (2, {3, 4, 1(marked red)})
		  for (IntWritable val : values) {
			  if (val.get() / mark_red < 0) {
				  //black color
				  dest.add(new Integer(val.get())); // 3, 4
			  } else {
				  //red color
				  srcs.add(new Integer(val.get() / mark_red)); // 1
			  }
		  }

		  for (Integer src : srcs) {
			  for (Integer dests: dest) {
				  context.write(new IntWritable(src.intValue()), new IntWritable(dests.intValue()));
			  }
		  }
      }
  }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 2) {
	    System.err.println("Usage: FriendsOfFriends <in> [<in>...] <out>");
	    System.exit(2);
	}

	Job job = Job.getInstance(conf, "Friends of Friends 2024 by LI Jinchuan 21252548");
	job.setJarByClass(FriendsOfFriends.class);
	job.setMapperClass(FriendMapper.class);
	job.setReducerClass(FriendReducer.class);
	job.setOutputKeyClass(IntWritable.class);
	job.setOutputValueClass(IntWritable.class);

	for (int i = 0; i < otherArgs.length - 1; ++i) {
	    FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
	}

	FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
