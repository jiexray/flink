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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Tests for the partitioned state part of {@link HashMapStateBackend}. */
public class HashMapStateBackendMigrationTest
        extends StateBackendMigrationTestBase<HashMapStateBackend> {

    @TempDir public static File tmpCheckpointPath;

    @Parameter(value = 1)
    public SupplierWithException<CheckpointStorage, IOException> storageSupplier;

    @Parameters
    public static List<Object[]> modes() throws Exception {
        ArrayList<Object[]> params = new ArrayList<>();
        params.add(
                new Object[] {
                    new HashMapStateBackend(),
                    (SupplierWithException<CheckpointStorage, IOException>)
                            JobManagerCheckpointStorage::new
                });
        params.add(
                new Object[] {
                    new HashMapStateBackend(),
                    (SupplierWithException<CheckpointStorage, IOException>)
                            () -> {
                                String checkpointPath = tmpCheckpointPath.toURI().toString();
                                return new FileSystemCheckpointStorage(checkpointPath);
                            }
                });
        return params;
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() throws Exception {
        return storageSupplier.get();
    }
}
