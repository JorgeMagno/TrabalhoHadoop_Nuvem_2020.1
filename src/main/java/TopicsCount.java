
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

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
import org.json.JSONObject;

public class TopicsCount {
	public static Set<String> words = new LinkedHashSet<String>();
	public static String[] stopwords = { "a", "as", "able", "about", "above", "according", "accordingly", "across",
			"actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone",
			"along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any",
			"anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate",
			"appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available",
			"away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before",
			"beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between",
			"beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause",
			"causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning",
			"consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could",
			"couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do",
			"does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight",
			"either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every",
			"everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few",
			"ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth",
			"four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going",
			"gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent",
			"having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein",
			"hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit",
			"however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc",
			"indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is",
			"isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows",
			"known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like",
			"liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me",
			"mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my",
			"myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never",
			"nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not",
			"nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on",
			"once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours",
			"ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps",
			"placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv",
			"rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively",
			"respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see",
			"seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious",
			"seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some",
			"somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon",
			"sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken",
			"tell", "tends", "th", "the", "than", "thank", "thanks", "thanx", "that", "thats", "their", "theirs",
			"them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein",
			"theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this",
			"thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to",
			"together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two",
			"un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used",
			"useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants",
			"was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent",
			"what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas",
			"whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos",
			"whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont",
			"wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your",
			"yours", "yourself", "yourselves" };

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);

		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String lineTokens = value.toString();
			try {
				JSONObject json = new JSONObject(lineTokens);
				String line = (String) json.get("title");
				String[] tokens = line.split(" ");
				if (tokens.length >= 3) {
					for (int i = 0; i < tokens.length - 2; i++) {
						tokens[i] = tokens[i].replace(".", "");
						tokens[i + 1] = tokens[i + 1].replace(".", "");
						tokens[i + 2] = tokens[i + 2].replace(".", "");
						if (tokens[i].toLowerCase().matches("[a-z]+\\.?")
								&& tokens[i + 1].toLowerCase().matches("[a-z]+\\.?")
								&& tokens[i + 2].toLowerCase().matches("[a-z]+\\.?")) {
							if (words.contains(tokens[i].toLowerCase()) && words.contains(tokens[i + 1].toLowerCase())
									&& words.contains(tokens[i + 2].toLowerCase())) {
								System.out.println("no sequence");
							} else {
								word.set(tokens[i].toLowerCase() + " " + tokens[i + 1].toLowerCase() + " "
										+ tokens[i + 2].toLowerCase());
								context.write(word, one);
								System.out.println(tokens[i] + " " + tokens[i + 1] + " " + tokens[i + 2]);
							}
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
		job.setJarByClass(TopicsCount.class);
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
		job2.setJarByClass(TopicsCount.class);
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
