/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.halo9pan.word2vec.hadoop.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SortSpliter {
	static String USE = "mapreduce.terasort.use.terascheduler";
	private static final Logger logger = LoggerFactory.getLogger(SortSpliter.class);
	private Split[] splits;
	private List<Host> hosts = new ArrayList<Host>();
	private int slotsPerHost;
	private int remainingSplits = 0;
	private FileSplit[] realSplits = null;

	static class Split {
		String filename;
		boolean isAssigned = false;
		List<Host> locations = new ArrayList<Host>();

		Split(String filename) {
			this.filename = filename;
		}

		public String toString() {
			StringBuffer result = new StringBuffer();
			result.append(filename);
			result.append(" on ");
			for (Host host : locations) {
				result.append(host.hostname);
				result.append(", ");
			}
			return result.toString();
		}
	}

	static class Host {
		String hostname;
		List<Split> splits = new ArrayList<Split>();

		Host(String hostname) {
			this.hostname = hostname;
		}

		public String toString() {
			StringBuffer result = new StringBuffer();
			result.append(splits.size());
			result.append(" ");
			result.append(hostname);
			return result.toString();
		}
	}

	public SortSpliter(FileSplit[] realSplits, Configuration conf) throws IOException {
		this.realSplits = realSplits;
		this.slotsPerHost = conf.getInt(TTConfig.TT_MAP_SLOTS, 4);
		Map<String, Host> hostTable = new HashMap<String, Host>();
		splits = new Split[realSplits.length];
		for (FileSplit realSplit : realSplits) {
			Split split = new Split(realSplit.getPath().toString());
			splits[remainingSplits++] = split;
			for (String hostname : realSplit.getLocations()) {
				Host host = hostTable.get(hostname);
				if (host == null) {
					host = new Host(hostname);
					hostTable.put(hostname, host);
					hosts.add(host);
				}
				host.splits.add(split);
				split.locations.add(host);
			}
		}
	}

	private Host findMinimalSplitsHost() {
		Host result = null;
		int splits = Integer.MAX_VALUE;
		for (Host host : hosts) {
			if (host.splits.size() < splits) {
				result = host;
				splits = host.splits.size();
			}
		}
		if (result != null) {
			hosts.remove(result);
			logger.debug("picking " + result);
		}
		return result;
	}

	private void pickBestSplits(Host host) {
		int pickTasks = Math.min(slotsPerHost, (int) Math.ceil((double) remainingSplits / hosts.size()));
		Split[] best = new Split[pickTasks];
		for (Split current : host.splits) {
			logger.debug("  examine: " + current.filename + " " + current.locations.size());
			int i = 0;
			while (i < pickTasks && best[i] != null && best[i].locations.size() <= current.locations.size()) {
				i += 1;
			}
			if (i < pickTasks) {
				for (int j = pickTasks - 1; j > i; --j) {
					best[j] = best[j - 1];
				}
				best[i] = current;
			}
		}
		// for the chosen blocks, remove them from the other locations
		for (int i = 0; i < pickTasks; ++i) {
			if (best[i] != null) {
				logger.debug(" best: " + best[i].filename);
				for (Host other : best[i].locations) {
					other.splits.remove(best[i]);
				}
				best[i].locations.clear();
				best[i].locations.add(host);
				best[i].isAssigned = true;
				remainingSplits -= 1;
			}
		}
		// for the non-chosen blocks, remove this host
		for (Split cur : host.splits) {
			if (!cur.isAssigned) {
				cur.locations.remove(host);
			}
		}
	}

	private void solve() throws IOException {
		Host host = findMinimalSplitsHost();
		while (host != null) {
			pickBestSplits(host);
			host = findMinimalSplitsHost();
		}
	}

	/**
	 * Solve the schedule and modify the FileSplit array to reflect the new
	 * schedule. It will move placed splits to front and unplacable splits
	 * to the end.
	 * 
	 * @return a new list of FileSplits that are modified to have the
	 *         best host as the only host.
	 * @throws IOException
	 */
	public List<InputSplit> reSplit() throws IOException {
		solve();
		FileSplit[] result = new FileSplit[realSplits.length];
		int left = 0;
		int right = realSplits.length - 1;
		for (int i = 0; i < splits.length; ++i) {
			if (splits[i].isAssigned) {
				// copy the split and fix up the locations
				String[] newLocations = { splits[i].locations.get(0).hostname };
				realSplits[i] = new FileSplit(realSplits[i].getPath(), realSplits[i].getStart(),
						realSplits[i].getLength(), newLocations);
				result[left++] = realSplits[i];
			} else {
				result[right--] = realSplits[i];
			}
		}
		List<InputSplit> ret = new ArrayList<InputSplit>();
		for (FileSplit fs : result) {
			ret.add(fs);
		}
		return ret;
	}

}
