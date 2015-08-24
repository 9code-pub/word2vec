/**
 * The MIT License (MIT)
 * 
 * Copyright (c) 2015 Halo9Pan
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package info.halo9pan.word2vec.hadoop;

import info.halo9pan.word2vec.hadoop.mr.ReadWordsMapper;
import info.halo9pan.word2vec.hadoop.mr.ReadWordsReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Halo9Pan
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("hadoop.tmp.dir", (new Path("temp")).toUri().toString());
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
			System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
			System.exit(2);
		}
		Path input = new Path(remainingArgs[0]);
		String inputName = input.getName();
		Path countOutput = new Path(input.getParent(), inputName + "_count");
		Job countJob = Job.getInstance(conf, "Word Count");
		countJob.setJarByClass(Main.class);
		countJob.setMapperClass(ReadWordsMapper.class);
		countJob.setCombinerClass(ReadWordsReducer.class);
		countJob.setReducerClass(ReadWordsReducer.class);
		countJob.setOutputKeyClass(Text.class);
		countJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(countJob, input);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(countOutput))
			fs.delete(countOutput, true);
		FileOutputFormat.setOutputPath(countJob, countOutput);
		countJob.waitForCompletion(true);

		System.exit(countJob.waitForCompletion(true) ? 0 : 1);
	}

}
