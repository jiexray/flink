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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.HashMapStateBackendTest;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Tests for {@link ChangelogStateBackend} delegating {@link HashMapStateBackendTest}. */
public class ChangelogDelegateHashMapTest extends HashMapStateBackendTest {

    @TempDir public static File tmPath;

    @Parameters(name = "statebackend={0}")
    public static List<Object[]> modes() {
        ArrayList<Object[]> params = new ArrayList<>();

        params.add(
                new Object[] {
                    new ChangelogStateBackend(new HashMapStateBackend()),
                    (SupplierWithException<CheckpointStorage, IOException>)
                            JobManagerCheckpointStorage::new
                });
        params.add(
                new Object[] {
                    new ChangelogStateBackend(new HashMapStateBackend()),
                    (SupplierWithException<CheckpointStorage, IOException>)
                            () -> {
                                String checkpointPath = tmpCheckpointPath.toURI().toString();
                                return new FileSystemCheckpointStorage(
                                        new Path(checkpointPath), 0, -1);
                            }
                });
        return params;
    }

    protected TestTaskStateManager getTestTaskStateManager() throws IOException {
        return ChangelogStateBackendTestUtils.createTaskStateManager(tmPath);
    }

    @Override
    protected boolean snapshotUsesStreamFactory() {
        return false;
    }

    @Override
    protected boolean supportsMetaInfoVerification() {
        return false;
    }

    @Override
    protected <K> CheckpointableKeyedStateBackend<K> createKeyedBackend(
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            Environment env)
            throws Exception {

        return ChangelogStateBackendTestUtils.createKeyedBackend(
                stateBackend, keySerializer, numberOfKeyGroups, keyGroupRange, env);
    }

    @TestTemplate
    public void testMaterializedRestore() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        ChangelogStateBackendTestUtils.testMaterializedRestore(
                stateBackend, StateTtlConfig.DISABLED, env, streamFactory);
    }

    @TestTemplate
    public void testMaterializedRestoreWithWrappedState() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.LATENCY_TRACK_ENABLED, true);
        stateBackend.configure(configuration, Thread.currentThread().getContextClassLoader());
        ChangelogStateBackendTestUtils.testMaterializedRestore(
                stateBackend,
                StateTtlConfig.newBuilder(Time.minutes(1)).build(),
                env,
                streamFactory);
    }

    @TestTemplate
    public void testMaterializedRestorePriorityQueue() throws Exception {
        CheckpointStreamFactory streamFactory = createStreamFactory();

        ChangelogStateBackendTestUtils.testMaterializedRestoreForPriorityQueue(
                stateBackend, env, streamFactory);
    }
}
