import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

public class MapTempCount {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lineTokens = value.toString();
			try {
				JSONObject json = new JSONObject(lineTokens);
				String line = (String) json.get("createdAt");
				String[] tokens = line.split(" ");
				for (int i = 2015; i < 2018; i++) {
					if (tokens[2].equals(String.valueOf(i))) {
						if (tokens[0].equals("January")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("February")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("March")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("April")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("May")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("June")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("July")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("August")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("September")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("October")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("November")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
						if (tokens[0].equals("December")) {
							String data = tokens[2] + " " + tokens[0];
							word.set(data);
							context.write(word, one);
						}
					}

				}

			} catch (Exception e) {
				// TODO: handle exception
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(MapTempCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
