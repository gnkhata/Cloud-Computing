
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
*	user:
*/

public class invertedindex {

	public static class TokenizerMapper extends Mapper<Object, Text, wordpair, Text>
	{
		HashMap<wordpair, Integer> count = new HashMap<wordpair, Integer>();

		private final static IntWritable one = new IntWritable(1);
		private wordpair wordPair = new wordpair();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{

			String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();

			StringTokenizer itr = new StringTokenizer(value.toString());
			Text word = new Text();
			while (itr.hasMoreTokens()) {
				wordpair checkKey = new wordpair();
				word.set(itr.nextToken());
				checkKey.setWord(word.toString());
				checkKey.setfileName(fileName);

				if(count.containsKey(checkKey)) {
					int sum = (int) count.get(checkKey) + 1;
					count.put(checkKey, sum);
				}
				else{
					count.put(checkKey, 1);
				}
			}//end of while
		} // end of map

		public void cleanup(Context context) throws IOException, InterruptedException
		{
			Iterator<HashMap.Entry<wordpair, Integer>> tmp = count.entrySet().iterator();

			while(tmp.hasNext()) {
				wordpair fileValPair = new wordpair();
				HashMap.Entry<wordpair, Integer> entry = tmp.next();
				wordpair keyVal = entry.getKey();
				Text wrd = keyVal.getWord();
				Text fNameT = keyVal.getfileName();
				String fName = fNameT.toString();
				Integer countVal = entry.getValue();

				fileValPair.setWord(wrd.toString());
				fileValPair.setfileName(fName);

				context.write(fileValPair, new Text(countVal.toString()));
			} // end of while
		} // end of cleanup
	}// end of TokenizerMapper class



	public static class IntSumReducer extends Reducer<wordpair,Text,Text,Text>
	{

		String combinedTerm ="";
		String Previous = "";

		public void reduce(wordpair key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
          //// insert your code here...



		}// end of reduce method

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(new Text(Previous), new Text(combinedTerm));
		}


	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
		System.err.println("Usage: invertedindex <in> <out>");
		System.exit(2);
		}
		Job job = new Job(conf, "inverted index");
		job.setNumReduceTasks(3);
		job.setJarByClass(invertedindex.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(wordpair.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}