package info.halo9pan.word2vec.hadoop.wc;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Map<String, String> env = System.getenv();
		System.out.println(env.get("Path"));
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration();
		conf.set("hadoop.tmp.dir", (new Path("tmp")).toUri().toString());

		// Create job
		Job job = Job.getInstance(conf, "WordCount");
		job.setJarByClass(WordCount.class);

		// Setup MapReduce
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(job, input);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, output);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output))
			fs.delete(output, true);

		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		System.exit(code);

	}

}