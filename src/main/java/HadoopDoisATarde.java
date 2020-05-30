import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopDoisATarde {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		Pattern pattern = Pattern.compile("#[a-zA-z0-9]+");
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineTokens = value.toString().split("\t");
			int h;
			h = Integer.valueOf(lineTokens[7].split(" ")[3].split(":")[0]);
			if (h >= 12 && h < 18) {
				Matcher matcher = pattern.matcher(lineTokens[1]);
				while (matcher.find()) {
					word.set(matcher.group().toLowerCase());
					context.write(word, one);
				}
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

	public static class TrendMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				context.write(new LongWritable(Long.parseLong(tokenizer.nextToken().toString())), new Text(token));

			}
		}

	}

	public static class TrendReducer extends Reducer<LongWritable, Text, Text, Text> {

		protected void reduce(LongWritable key, Iterable<Text> trends, Context context)
				throws IOException, InterruptedException {

			for (Text val : trends) {
				context.write(new Text(val.toString()), new Text(key.toString()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(HadoopDoisATarde.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		Job job2 = new Job(conf, "word count");
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setJarByClass(HadoopDoisATarde.class);
		job2.setMapperClass(TrendMapper.class);
		job2.setReducerClass(TrendReducer.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		System.exit(job2.waitForCompletion(true) ? 1 : 2);
	}
}