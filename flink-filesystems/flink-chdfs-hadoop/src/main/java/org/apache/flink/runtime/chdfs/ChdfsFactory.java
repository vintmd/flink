/*
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

package org.apache.flink.runtime.chdfs;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystemFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.HadoopConfigLoader;

import com.qcloud.chdfs.fs.CHDFSHadoopFileSystem;
// import chdfs.0.6.6.com.qcloud.chdfs.fs.CHDFSHadoopFileSystem;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A factory for the Chdfs file system.
 */
public class ChdfsFactory implements FileSystemFactory {

	private static final Logger LOG = LoggerFactory.getLogger(ChdfsFactory.class);

	private static final String[] FLINK_CONFIG_PREFIXES = { "fs.ofs.", "ofs." };

	private static final String HADOOP_CONFIG_PREFIX = "fs.ofs.";

	private static final String[][] MIRRORED_CONFIG_KEYS = {};

	private static final Set<String> PACKAGE_PREFIXES_TO_SHADE = Collections.emptySet();

	private static final Set<String> CONFIG_KEYS_TO_SHADE = Collections.emptySet();

	private static final String FLINK_SHADING_PREFIX = "";

	private Configuration flinkConfig;

	private final HadoopConfigLoader configLoader;

	public ChdfsFactory() {
		LOG.info("begin the construct of chdfs factory");
		this.configLoader = new HadoopConfigLoader(FLINK_CONFIG_PREFIXES, MIRRORED_CONFIG_KEYS,
			HADOOP_CONFIG_PREFIX, PACKAGE_PREFIXES_TO_SHADE, CONFIG_KEYS_TO_SHADE, FLINK_SHADING_PREFIX);
	}

	@Override
	public void configure(Configuration config) {
		this.flinkConfig = config;
		configLoader.setFlinkConfig(config);
	}

	@Override
	public String getScheme() {
		return "ofs";
	}

	@Override
	public FileSystem create(URI fsUri) throws IOException {
		checkNotNull(fsUri, "passed file system URI object should not be null");
		LOG.info("Creating the Chdfs FileSystem.");
		return new ChdfsFileSystem(createInitializedChdfs(fsUri, flinkConfig));
	}

	// uri is of the form: ofs://f4mmzj1iiio-ABCD.chdfs.ap-beijing.myqcloud.com/
	private org.apache.hadoop.fs.FileSystem createInitializedChdfs(URI fsUri, Configuration flinkConfig) throws IOException {
		org.apache.hadoop.conf.Configuration hadoopConfig = configLoader.getOrLoadHadoopConfig();

		org.apache.hadoop.fs.FileSystem chdfsFileSystem = new CHDFSHadoopFileSystem();
		chdfsFileSystem.initialize(fsUri, hadoopConfig);
		return chdfsFileSystem;
	}
}
