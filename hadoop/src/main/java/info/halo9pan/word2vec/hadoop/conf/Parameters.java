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
package info.halo9pan.word2vec.hadoop.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum Parameters {

	INSTANCE;

	private static Logger logger = LoggerFactory.getLogger(Parameters.class);

	private static final String DEFAULT_CONFIGURATION_FILE = "parameters.properties";
	private static final String PARAMETER_KEY_ALPHA = "alpha";
	private static final String PARAMETER_KEY_WINDOW = "window";

	private static final float PARAMETER_DEFAULT_ALPHA = 0.025f;
	private static final int PARAMETER_DEFAULT_WINDOW = 5;

	private Configuration configuration;

	private Parameters() {
		initialize();
	}

	public void initialize() {
		this.initialize(DEFAULT_CONFIGURATION_FILE);
	}

	public void initialize(String fileName) {
		try {
			if (fileName == null) {
				fileName = DEFAULT_CONFIGURATION_FILE;
			}
			this.configuration = new PropertiesConfiguration(fileName);
		} catch (ConfigurationException e) {
			logger.error("Can't initialize the properties configuration.");
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 梯度步长
	 * @return
	 */
	public float getAlpha() {
		return this.configuration.getFloat(PARAMETER_KEY_ALPHA, PARAMETER_DEFAULT_ALPHA);
	}

	/**
	 * 窗口大小
	 * @return
	 */
	public int getWindow() {
		return this.configuration.getInt(PARAMETER_KEY_WINDOW, PARAMETER_DEFAULT_WINDOW);
	}

}
