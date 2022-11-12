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

package org.apache.flink.state.changelog;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.HashMapStateBackendTest;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;

import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Rule;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link ChangelogStateBackend} using {@link InMemoryStateChangelogStorage} and
 * delegating {@link HashMapStateBackendTest}.
 */
public class ChangelogDelegateHashMapInMemoryTest extends ChangelogDelegateHashMapTest {

    @TempDir
    public static java.nio.file.Path tmp;

    @Parameters(name = "statebackend={0}")
    public static List<Object[]> modes() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(new Object[] {
                new ChangelogStateBackend(new HashMapStateBackend()),
                (SupplierWithException<CheckpointStorage, IOException>)
                        JobManagerCheckpointStorage::new
        });
        params.add(new Object[] {
                new ChangelogStateBackend(new HashMapStateBackend()),
                (SupplierWithException<CheckpointStorage, IOException>)
                        () -> {
                            String checkpointPath =
                                    new File(tmp.toFile(), "checkpointPath").toURI().toString();
                            return new FileSystemCheckpointStorage(
                                    new Path(checkpointPath), 0, -1);
                        }
        });
        return params;
    }

    protected TestTaskStateManager getTestTaskStateManager() {
        return TestTaskStateManager.builder().build();
    }
}
