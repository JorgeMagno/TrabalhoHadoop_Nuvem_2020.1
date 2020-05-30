
import java.io.IOException;
import java.text.Normalizer;
import java.util.LinkedHashSet;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AecioCount {
	public static Set<String> words = new LinkedHashSet<String>();
	public static String[] stopwords = { "a", "às", "algum", "alguma", "algumas", "alguns", "ampla", "amplas", "amplo",
			"amplos", "ao", "aos", "aquela", "aquelas", "aquele", "aqueles", "aquilo", "as", "cada", "com", "como",
			"contra", "contudo", "da", "daquele", "daqueles", "das", "de", "dela", "delas", "dele", "deles", "dessa",
			"dessas", "desse", "desses", "desta", "destas", "deste", "destes", "deve", "devem", "devendo", "dever",
			"deverá", "deverão", "deveria", "deveriam", "disso", "disto", "do", "dos", "e", "é", "em", "enquanto",
			"entre", "era", "essa", "essas", "esse", "esses", "esta", "estas", "eh", "este", "estes", "fazendo", "fazer",
			"feita", "feitas", "feito", "feitos", "ha", "há", "isso", "isto", "ja", "já", "la", "lá", "lhe", "lhes",
			"lo", "mas", "me", "mesma", "mesmas", "mesmo", "mesmos", "meu", "meus", "muita", "muitas", "muito",
			"muitos", "na", "nas", "nem", "nenhum", "nessa", "nessas", "nesta", "nestas", "no", "nos", "num", "numa",
			"o", "os", "ou", "outra", "outras", "outro", "outros", "para", "pela", "pelas", "pelo", "pelos", "per",
			"perante", "pude", "podia", "podiam", "pois", "por", "porém", "pro", "posso", "pouca", "poucas", "pouco", "poucos",
			"pra", "própria", "próprias", "próprio", "próprios", "quais", "qual", "quando", "quanto", "quantos", "q",
			"que", "quem", "são", "se", "sendo", "será", "serão", "si", "sido", "so", "só", "sob", "sobre", "sua",
			"suas", "ta", "talvez", "também", "te", "tem", "tendo", "tenha", "ter", "teu", "teus", "ti", "tido",
			"tinha", "tinham", "toda", "todas", "todavia", "todo", "todos", "tua", "tuas", "tudo", "um", "uma", "umas",
			"uns", "vendo", "ver", "vindo", "vir", "vos", "vós", "vai" };

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		Pattern pattern1 = Pattern.compile("[a-z0-9]+ \\baecio\\b");
		Pattern pattern2 = Pattern.compile("\\baecio\\b [a-z0-9]+");
		Pattern pattern3 = Pattern.compile("[a-z0-9]+ \\baecio\\b [a-z0-9]+");
		Pattern pattern4 = Pattern.compile(" [a-z0-9]+ [a-z0-9]+ \\baecio\\b");
		Pattern pattern5 = Pattern.compile("\\aecio\\b [a-z0-9]+ [a-z0-9]+ ");
		Pattern pattern6 = Pattern.compile("\\aecio\\b [a-z0-9]+ [a-z0-9]+ [a-z0-9]+");
		Pattern pattern7 = Pattern.compile("[a-z0-9]+ [a-z0-9]+ [a-z0-9]+ \\\\baecio\\\\b");

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] lineTokens = value.toString().split("\t");
			String mydata = lineTokens[1].replaceAll("  *", " ");
			mydata = stripAccents(mydata);
			Matcher matcher1 = pattern1.matcher(mydata);
			Matcher matcher2 = pattern2.matcher(mydata);
			Matcher matcher3 = pattern3.matcher(mydata);
			Matcher matcher4 = pattern4.matcher(mydata);
			Matcher matcher5 = pattern5.matcher(mydata);
			Matcher matcher6 = pattern6.matcher(mydata);
			Matcher matcher7 = pattern7.matcher(mydata);
			String aux;

			while (matcher1.find()) {
				aux = matcher1.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[0])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}
				}
			}

			while (matcher2.find()) {
				aux = matcher2.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[1])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}
				}
			}

			while (matcher3.find()) {
				aux = matcher3.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[0]) && words.contains(sequence[2])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}

				}
			}
			while (matcher4.find()) {
				aux = matcher4.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[0]) && words.contains(sequence[1])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}

				}
			}
			while (matcher5.find()) {
				aux = matcher5.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[1]) && words.contains(sequence[2])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}
				}
			}
			while (matcher6.find()) {
				aux = matcher6.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[1]) && words.contains(sequence[2]) && words.contains(sequence[3])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}
				}
			}
			while (matcher7.find()) {
				aux = matcher7.group().trim();
				if (aux.length() > 7) {
					String[] sequence = aux.split(" ");
					if (words.contains(sequence[0]) && words.contains(sequence[1]) && words.contains(sequence[2])) {
						System.out.println("no sequence");
					} else {
						word.set(aux);
						context.write(word, one);
						System.out.println(aux);
					}
				}
			}
		}

		public static String stripAccents(String word) {
			word = Normalizer.normalize(word, Normalizer.Form.NFD);
			word = word.replaceAll("[\\p{InCombiningDiacriticalMarks}]", "");
			return word.toLowerCase();
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
			StringTokenizer tokenizer = new StringTokenizer(line, "\t");
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
		for (String w : stopwords) {
			words.add(w);
			System.out.println(w);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(AecioCount.class);
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
		job2.setJarByClass(AecioCount.class);
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
