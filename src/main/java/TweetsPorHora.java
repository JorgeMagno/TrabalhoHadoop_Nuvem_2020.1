import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TweetsPorHora {
	public static Set<String> days = new LinkedHashSet<String>();
	public static String currentDay;

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineTokens = value.toString().split("\t");
			System.out.println(lineTokens[8]);
			days.add(lineTokens[8]);
		}
	}

	public static class TokenizerMapperDays extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineTokens = value.toString().split("\t");
			String[] line = new String[24];
			int h;
			if (lineTokens[8].equals(currentDay)) {
				h = Integer.valueOf(lineTokens[7].split(" ")[3].split(":")[0]);
				for (int i = 0; i < line.length; i++) {
					if (h == i) {
						line[i] = "hora " + i;
						word.set(line[i]);
					}
				}
				context.write(word, one);
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

	public static void extractDays(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TweetsPorHora.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}

	public static void Informations(String input, String output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(TweetsPorHora.class);
		job.setMapperClass(TokenizerMapperDays.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		extractDays(args[0], args[1]);
		for (String day : days) {
			System.out.println(">>>>" + day);
			currentDay = day;
			Informations(args[0], args[1] + "/" + day.replaceAll("\"", ""));
		}

	}

}
