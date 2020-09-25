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

import org.apache.flink.core.fs.FileSystemKind;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class ChdfsFileSystem extends HadoopFileSystem {
	/**
	 * Wraps the given Hadoop File System object as a Flink File System object.
	 * The given Hadoop file system object is expected to be initialized already.
	 *
	 * @param hadoopFileSystem The Hadoop FileSystem that will be used under the hood.
	 */
	public ChdfsFileSystem(FileSystem hadoopFileSystem) {
		super(hadoopFileSystem);
	}

	@Override
	public FileSystemKind getKind() {
		return FileSystemKind.FILE_SYSTEM;
	}

	@Override
	public RecoverableWriter createRecoverableWriter() throws IOException {
		return new ChdfsRecoverableWriter(getHadoopFileSystem());
	}

	// utilities
	public static org.apache.hadoop.fs.Path toHadoopPath(Path path) {
		return new org.apache.hadoop.fs.Path(path.toUri());
	}


}
