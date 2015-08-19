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
package info.halo9pan.word2vec.hadoop.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Halo9Pan
 *
 */
public class ReadWordsMapper extends Mapper<Object, Text, Text, IntWritable> implements ValueIgnorable<String> {

	private static Logger logger = LoggerFactory.getLogger(ReadWordsMapper.class);

	static enum WordsCounter {
		InputWords, SkipWords
	}

	private final static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	private Set<String> skipPatterns = new HashSet<String>();

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		float progress = context.getProgress();
		logger.debug("Process: {}", progress);
	}

	@Override
	public Set<String> getIgnoreSet() {
		return this.skipPatterns;
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		for (String pattern : skipPatterns) {
			if (line.contains(pattern)) {
				Counter counter = context.getCounter(WordsCounter.SkipWords.toString(), pattern);
				counter.increment(1);
			}
			line = line.replaceAll(pattern, StringUtils.EMPTY);
		}
		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, ONE);
			Counter counter = context.getCounter(WordsCounter.class.getName(), WordsCounter.InputWords.toString());
			counter.increment(1);
		}
	}
}
