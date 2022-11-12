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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.FileStateBackendTest;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/** Tests for {@link ChangelogStateBackend} delegating {@link FsStateBackend}. */
public class ChangelogDelegateFileStateBackendTest extends FileStateBackendTest {

    @TempDir
    public static java.nio.file.Path tmp;

    @Parameters(name = "statebackend={0}, useAsyncMode={1}")
    public static Collection<Object[]> modes() {
        ArrayList<Object[]> params = new ArrayList<>();
        for (boolean useAsyncMode : Arrays.asList(true, false)) {
            File checkpointPath = new File(tmp.toFile(), "checkpointPath");
            params.add(
                    new Object[] {
                            new ChangelogStateBackend(new FsStateBackend(checkpointPath.toURI(), useAsyncMode)), useAsyncMode
                    });
        }
        return params;
    }

    @Override
    protected TestTaskStateManager getTestTaskStateManager() throws IOException {
        return ChangelogStateBackendTestUtils.createTaskStateManager(new File(tmp.toFile(), "tmpPath"));
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
                stateBackend,
                keySerializer,
                numberOfKeyGroups,
                keyGroupRange,
                env);
    }

    @Override
    protected CheckpointStorage getCheckpointStorage() {
        return new JobManagerCheckpointStorage();
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
