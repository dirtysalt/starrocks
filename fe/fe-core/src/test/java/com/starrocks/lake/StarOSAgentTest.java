// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.staros.client.StarClient;
import com.staros.client.StarClientException;
import com.staros.proto.CreateShardGroupInfo;
import com.staros.proto.CreateShardInfo;
import com.staros.proto.FileCacheInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.ReplicaInfo;
import com.staros.proto.ReplicaRole;
import com.staros.proto.ReplicationType;
import com.staros.proto.S3FileStoreInfo;
import com.staros.proto.ShardGroupInfo;
import com.staros.proto.ShardInfo;
import com.staros.proto.StarStatus;
import com.staros.proto.StatusCode;
import com.staros.proto.WarmupLevel;
import com.staros.proto.WorkerGroupDetailInfo;
import com.staros.proto.WorkerGroupSpec;
import com.staros.proto.WorkerInfo;
import com.staros.proto.WorkerState;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarOSAgentTest {
    private StarOSAgent starosAgent;
    public static final String SERVICE_NAME = "starrocks";

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    SystemInfoService service;

    @Mocked
    StarClient client;

    @BeforeEach
    public void setUp() throws Exception {
        starosAgent = new StarOSAgent();
        starosAgent.initForTest();
        Config.cloud_native_storage_type = "S3";
    }

    @Test
    public void testRegisterAndBootstrapService() throws Exception {
        new Expectations() {
            {
                client.registerService(SERVICE_NAME);
                minTimes = 0;
                result = null;

                client.bootstrapService("starrocks", SERVICE_NAME);
                minTimes = 0;
                result = "1";
            }
        };

        starosAgent.registerAndBootstrapService();
        Assertions.assertEquals("1", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testRegisterServiceException() throws Exception {
        new Expectations() {
            {
                client.registerService(SERVICE_NAME);
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST,
                        "service already exists!");

                client.bootstrapService("starrocks", SERVICE_NAME);
                minTimes = 0;
                result = "3";
            }
        };

        starosAgent.registerAndBootstrapService();
        Assertions.assertEquals("3", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testBootstrapServiceException() throws Exception {
        new Expectations() {
            {
                client.bootstrapService("starrocks", SERVICE_NAME);
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST,
                        "service already exists!");

                client.getServiceInfoByName(SERVICE_NAME).getServiceId();
                minTimes = 0;
                result = "4";
            }
        };

        starosAgent.registerAndBootstrapService();
        Assertions.assertEquals("4", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testGetServiceId() throws Exception {
        new Expectations() {
            {
                client.getServiceInfoByName(SERVICE_NAME).getServiceId();
                minTimes = 0;
                result = "2";
            }
        };

        starosAgent.getServiceId();
        Assertions.assertEquals("2", Deencapsulation.getField(starosAgent, "serviceId"));
    }

    @Test
    public void testAllocateFilePath() throws StarClientException {
        long dbId = 1000;
        long tableId = 123;

        new Expectations() {
            {
                client.allocateFilePath("1", FileStoreType.S3, anyString);
                result = FilePathInfo.newBuilder().build();
                minTimes = 0;

                client.allocateFilePath("2", FileStoreType.S3, anyString);
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Config.cloud_native_storage_type = "s3";
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.allocateFilePath(dbId, tableId));

        Config.cloud_native_storage_type = "ss";
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Invalid cloud native storage type: ss",
                () -> starosAgent.allocateFilePath(dbId, tableId));

        Config.cloud_native_storage_type = "s3";
        Deencapsulation.setField(starosAgent, "serviceId", "2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to allocate file path from StarMgr, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.allocateFilePath(dbId, tableId));

        new Expectations() {
            {
                client.allocateFilePath("1", "test-fskey", anyString);
                result = FilePathInfo.newBuilder().build();
                minTimes = 0;

                client.allocateFilePath("2", "test-fskey", anyString);
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };
        Config.cloud_native_storage_type = "s3";
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.allocateFilePath("test-fskey", dbId, tableId));

        Deencapsulation.setField(starosAgent, "serviceId", "2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to allocate file path from StarMgr, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.allocateFilePath("test-fskey", dbId, tableId));
    }

    @Test
    public void testAddAndRemoveWorker() throws Exception {
        new Expectations() {
            {
                client.addWorker("1", "127.0.0.1:8090", StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = 10;

                client.removeWorker("1", 10);
                minTimes = 0;
                result = null;
            }
        };

        String workerHost = "127.0.0.1:8090";
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.addWorker(5, workerHost, 0);
        Assertions.assertEquals(10, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorker(workerHost, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        Assertions.assertEquals(-1, starosAgent.getWorkerIdByNodeId(5));
    }

    @Test
    public void testAddWillRemovePreviousWorker() throws Exception {
        final String workerHost = "127.0.0.1:8090";
        final long workerId1 = 10;
        final long workerId2 = 11;
        new Expectations() {
            {
                client.addWorker("1", workerHost, 0);
                minTimes = 1;
                result = workerId1;
            }
        };

        long backendId = 5;
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.addWorker(backendId, workerHost, 0);
        Assertions.assertEquals(workerId1, starosAgent.getWorkerIdByNodeId(backendId));

        final String workerHost2 = "127.0.0.1:8091";
        new Expectations() {
            {
                client.addWorker("1", workerHost2, StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 1;
                result = workerId2;

                client.removeWorker("1", workerId1);
                minTimes = 1;
                result = null;
            }
        };
        starosAgent.addWorker(backendId, workerHost2, 0);
        Assertions.assertEquals(workerId2, starosAgent.getWorkerIdByNodeId(backendId));
    }

    @Test
    public void testAddWorkerException() throws Exception {
        new Expectations() {
            {
                client.addWorker("1", "127.0.0.1:8090", StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST, "worker already exists");

                client.getWorkerInfo("1", "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 6;
            }
        };

        String workerHost = "127.0.0.1:8090";
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.addWorker(5, workerHost, 0);
        Assertions.assertEquals(6, starosAgent.getWorkerId(workerHost));
        Assertions.assertEquals(6, starosAgent.getWorkerIdByNodeId(5));

        new Expectations() {
            {
                client.addWorker("1", "127.0.0.1:8091", StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = new StarClientException(StatusCode.ALREADY_EXIST,
                        "worker already exists");

                client.getWorkerInfo("1", "127.0.0.1:8091").getWorkerId();
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC,
                        "network error");
            }
        };
        starosAgent.addWorker(10, "127.0.0.1:8091", 0);
        ExceptionChecker.expectThrows(NullPointerException.class,
                () -> starosAgent.getWorkerId("127.0.0.1:8091"));
    }

    @Test
    public void testRemoveWorkerException() throws Exception {
        new Expectations() {
            {
                client.getWorkerInfo("1", "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC, "network error");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to get worker id from starMgr.",
                () -> starosAgent.removeWorker("127.0.0.1:8090", StarOSAgent.DEFAULT_WORKER_GROUP_ID));

        new Expectations() {
            {
                client.getWorkerInfo("1", "127.0.0.1:8090").getWorkerId();
                minTimes = 0;
                result = 10;

                client.removeWorker("1", 10, 0);
                minTimes = 0;
                result = new StarClientException(StatusCode.GRPC, "network error");
            }
        };

        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to remove worker.",
                () -> starosAgent.removeWorker("127.0.0.1:8090", StarOSAgent.DEFAULT_WORKER_GROUP_ID));
    }

    @Test
    public void testCreateAndListShardGroup() throws StarClientException, DdlException {
        ShardInfo shard1 = ShardInfo.newBuilder().setShardId(10L).build();
        ShardInfo shard2 = ShardInfo.newBuilder().setShardId(11L).build();
        List<ShardInfo> shards = Lists.newArrayList(shard1, shard2);

        long groupId = 333;
        ShardGroupInfo info = ShardGroupInfo.newBuilder().setGroupId(groupId).build();
        List<ShardGroupInfo> groups = new ArrayList<>(1);
        groups.add(info);

        new MockUp<StarClient>() {
            @Mock
            public List<ShardInfo> createShard(String serviceId, List<CreateShardInfo> createShardInfos)
                    throws StarClientException {
                return shards;
            }

            @Mock
            public List<ShardGroupInfo> createShardGroup(String serviceId, List<CreateShardGroupInfo> createShardGroupInfos)
                    throws StarClientException {
                return groups;
            }

            @Mock
            public List<ShardGroupInfo> listShardGroup(String serviceId) throws StarClientException {
                return groups;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        // test create shard group
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.createShardGroup(0, 0, 1, 1));
        // test create shards
        FilePathInfo pathInfo = FilePathInfo.newBuilder().build();
        FileCacheInfo cacheInfo = FileCacheInfo.newBuilder().build();
        Assertions.assertEquals(Lists.newArrayList(10L, 11L),
                starosAgent.createShards(2, pathInfo, cacheInfo, 333, null,
                Collections.EMPTY_MAP, WarehouseManager.DEFAULT_RESOURCE));

        // list shard group
        List<ShardGroupInfo> realGroupIds = starosAgent.listShardGroup();
        Assertions.assertEquals(1, realGroupIds.size());
        Assertions.assertEquals(groupId, realGroupIds.get(0).getGroupId());
    }

    @Test
    public void testDeleteShardGroup() throws StarClientException, DdlException {
        new Expectations() {
            {
                client.deleteShardGroup("1", (List<Long>) any, true);
                minTimes = 0;
                result = null;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");

        // test delete shard group
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.deleteShardGroup(Lists.newArrayList(1L, 2L)));
    }

    @Test
    public void testGetBackendByShard() throws StarClientException, StarRocksException {
        ReplicaInfo replica1 = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.PRIMARY)
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(1L).setWorkerState(WorkerState.ON).build())
                .build();
        ReplicaInfo replica2 = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.PRIMARY)
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(2L).setWorkerState(WorkerState.ON).build())
                .build();
        ReplicaInfo replica3 = ReplicaInfo.newBuilder()
                .setReplicaRole(ReplicaRole.PRIMARY)
                .setWorkerInfo(WorkerInfo.newBuilder().setWorkerId(3L).setWorkerState(WorkerState.OFF).build())
                .build();
        List<ReplicaInfo> replicas = Lists.newArrayList(replica1, replica2, replica3);

        ShardInfo shard = ShardInfo.newBuilder().setShardId(10L).addAllReplicaInfo(replicas).build();
        List<ShardInfo> shards = Lists.newArrayList(shard);

        /*
        new MockUp<GlobalStateMgr>() {
            @Mock
            public SystemInfoService getCurrentState().getNodeMgr().getClusterInfo() {
                return service;
            }
        };

         */

        new MockUp<SystemInfoService>() {
            @Mock
            public long getBackendIdWithStarletPort(String host, int starletPort) {
                return -1L;
            }

            @Mock
            public long getComputeNodeIdWithStarletPort(String host, int starletPort) {
                return -1L;
            }
        };

        new MockUp<WorkerInfo>() {
            @Mock
            public String getIpPort() {
                return "127.0.0.1:8090";
            }
        };

        new Expectations() {
            {
                client.getShardInfo("1", Lists.newArrayList(10L), StarOSAgent.DEFAULT_WORKER_GROUP_ID);
                minTimes = 0;
                result = shards;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Map<Long, Long> workerToNode = Maps.newHashMap();
        Deencapsulation.setField(starosAgent, "workerToNode", workerToNode);

        ExceptionChecker.expectThrowsWithMsg(StarRocksException.class,
                "Failed to get primary backend. shard id: 10",
                () -> starosAgent.getPrimaryComputeNodeIdByShard(10L, StarOSAgent.DEFAULT_WORKER_GROUP_ID));

        Assertions.assertEquals(Lists.newArrayList(),
                starosAgent.getAllNodeIdsByShard(10L, StarOSAgent.DEFAULT_WORKER_GROUP_ID));

        workerToNode.put(1L, 10001L);
        workerToNode.put(2L, 10002L);
        workerToNode.put(3L, 10003L);
        Deencapsulation.setField(starosAgent, "workerToNode", workerToNode);

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assertions.assertEquals(10001L, starosAgent.getPrimaryComputeNodeIdByShard(10L,
                StarOSAgent.DEFAULT_WORKER_GROUP_ID));
        Assertions.assertEquals(Lists.newArrayList(10001L, 10002L, 10003L),
                starosAgent.getAllNodeIdsByShard(10L, StarOSAgent.DEFAULT_WORKER_GROUP_ID));
    }

    @Test
    public void testRemoveWorkerFromMap() {
        String workerHost = "127.0.0.1:8090";
        Map<String, Long> mockWorkerToId = Maps.newHashMap();
        mockWorkerToId.put(workerHost, 5L);
        Deencapsulation.setField(starosAgent, "workerToId", mockWorkerToId);
        Assertions.assertEquals(5L, starosAgent.getWorkerId(workerHost));

        starosAgent.removeWorkerFromMap(5L, workerHost);
        ExceptionChecker.expectThrows(NullPointerException.class, () -> starosAgent.getWorkerId(workerHost));
    }

    private WorkerInfo newWorkerInfo(long workerId, String ipPort, int beHeartbeatPort, int bePort, int beHttpPort,
                                     int beBrpcPort, int tabletNum) {
        return WorkerInfo.newBuilder().setWorkerId(workerId).setIpPort(ipPort)
                .putWorkerProperties("be_heartbeat_port", String.valueOf(beHeartbeatPort))
                .putWorkerProperties("be_port", String.valueOf(bePort))
                .putWorkerProperties("be_http_port", String.valueOf(beHttpPort))
                .putWorkerProperties("be_brpc_port", String.valueOf(beBrpcPort))
                .setTabletNum(tabletNum)
                .build();
    }

    @Test
    public void testGetWorkers() throws StarClientException, StarRocksException {
        String serviceId = "1";
        Deencapsulation.setField(starosAgent, "serviceId", serviceId);

        long workerId0 = 10000L;
        WorkerInfo worker0 = newWorkerInfo(workerId0, "127.0.0.1:8090", 9050, 9060, 8040, 8060, 10);
        long workerId1 = 10001L;
        WorkerInfo worker1 = newWorkerInfo(workerId1, "127.0.0.2:8091", 9051, 9061, 8041, 8061, 10);
        long groupId0 = 10L;
        WorkerGroupDetailInfo group0 = WorkerGroupDetailInfo.newBuilder().setGroupId(groupId0).addWorkersInfo(worker0)
                .addWorkersInfo(worker1).build();

        long workerId2 = 10002L;
        WorkerInfo worker2 = newWorkerInfo(workerId2, "127.0.0.3:8092", 9052, 9062, 8042, 8062, 10);
        long groupId1 = 11L;
        WorkerGroupDetailInfo group1 = WorkerGroupDetailInfo.newBuilder().setGroupId(groupId1).addWorkersInfo(worker2)
                .build();

        new Expectations() {
            {
                client.getWorkerInfo(serviceId, workerId0);
                minTimes = 0;
                result = worker0;

                client.listWorkerGroup(serviceId, Lists.newArrayList(groupId0), true);
                minTimes = 0;
                result = Lists.newArrayList(group0);

                client.listWorkerGroup(serviceId, Lists.newArrayList(), true);
                minTimes = 0;
                result = Lists.newArrayList(group0, group1);
            }
        };

        List<Long> nodes = starosAgent.getWorkersByWorkerGroup(groupId0);
        Assertions.assertEquals(2, nodes.size());
    }

    @Test
    public void testGetWorkerTabletNum() throws StarClientException {
        String serviceId = "1";
        String workerIpPort = "127.0.0.1:8093";
        long workerId = 20000L;
        int expectedTabletNum = 10086;
        Deencapsulation.setField(starosAgent, "serviceId", serviceId);
        WorkerInfo worker = newWorkerInfo(workerId, workerIpPort, 9050, 9060, 8040, 8060, expectedTabletNum);

        new Expectations() {
            {
                client.getWorkerInfo(serviceId, workerIpPort);
                minTimes = 1;
                result = worker;
            }
        };
        long tabletNum = starosAgent.getWorkerTabletNum(workerIpPort);
        Assertions.assertEquals(expectedTabletNum, worker.getTabletNum());
        Assertions.assertEquals(expectedTabletNum, tabletNum);
    }

    @Test
    public void testGetWorkerTabletNumExcepted() throws StarClientException {
        String serviceId = "1";
        String workerIpPort = "127.0.0.1:8093";
        Deencapsulation.setField(starosAgent, "serviceId", serviceId);

        new Expectations() {
            {
                client.getWorkerInfo(serviceId, anyString);
                result = new StarClientException(
                        StarStatus.newBuilder().setStatusCode(StatusCode.INTERNAL).setErrorMsg("injected error")
                                .build());
                minTimes = 1;
            }
        };

        ExceptionChecker.expectThrowsNoException(() -> {
            // no exception at all, return 0 instead
            long tabletNum = starosAgent.getWorkerTabletNum(workerIpPort);
            Assertions.assertEquals(0, tabletNum);
        });
    }

    @Test
    public void testAddFileStore() throws StarClientException, DdlException {
        S3FileStoreInfo s3FsInfo = S3FileStoreInfo.newBuilder()
                .setRegion("region").setEndpoint("endpoint").build();
        FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsKey("test-fskey")
                .setFsName("test-fsname").setFsType(FileStoreType.S3).setS3FsInfo(s3FsInfo).build();
        new Expectations() {
            {
                client.addFileStore(fsInfo, "1");
                result = fsInfo.getFsKey();
                minTimes = 0;

                client.addFileStore(fsInfo, "2");
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assertions.assertEquals("test-fskey", starosAgent.addFileStore(fsInfo));

        Deencapsulation.setField(starosAgent, "serviceId", "2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to add file store, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.addFileStore(fsInfo));
    }

    @Test
    public void testListFileStore() throws StarClientException, DdlException {
        S3FileStoreInfo s3FsInfo = S3FileStoreInfo.newBuilder()
                .setRegion("region").setEndpoint("endpoint").build();
        FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsKey("test-fskey")
                .setFsName("test-fsname").setFsType(FileStoreType.S3).setS3FsInfo(s3FsInfo).build();
        new Expectations() {
            {
                client.listFileStore("1");
                result = new ArrayList<>(Arrays.asList(fsInfo));
                minTimes = 0;

                client.listFileStore("2");
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assertions.assertEquals(1, starosAgent.listFileStore().size());
        Assertions.assertEquals("test-fskey", starosAgent.listFileStore().get(0).getFsKey());

        Deencapsulation.setField(starosAgent, "serviceId", "2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to list file store, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.listFileStore());
    }

    @Test
    public void testUpdateFileStore() throws StarClientException, DdlException {
        S3FileStoreInfo s3FsInfo = S3FileStoreInfo.newBuilder()
                .setRegion("region").setEndpoint("endpoint").build();
        FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsKey("test-fskey")
                .setFsName("test-fsname").setFsType(FileStoreType.S3).setS3FsInfo(s3FsInfo).build();
        new Expectations() {
            {
                client.updateFileStore(fsInfo, "1");
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to update file store, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.updateFileStore(fsInfo));
    }

    @Test
    public void testRemoveFileStoreByName() throws StarClientException, DdlException {
        new Expectations() {
            {
                client.removeFileStoreByName("test-fsname", "1");
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to remove file store, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.removeFileStoreByName("test-fsname"));
    }

    @Test
    public void testGetFileStoreByName() throws StarClientException, DdlException {
        S3FileStoreInfo s3FsInfo = S3FileStoreInfo.newBuilder()
                .setRegion("region").setEndpoint("endpoint").build();
        FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsKey("test-fskey")
                .setFsName("test-fsname").setFsType(FileStoreType.S3).setS3FsInfo(s3FsInfo).build();
        new Expectations() {
            {
                client.getFileStoreByName("test-fsname", "1");
                result = fsInfo;
                minTimes = 0;

                client.getFileStoreByName("test-fsname", "2");
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assertions.assertEquals("test-fskey", starosAgent.getFileStoreByName("test-fsname").getFsKey());

        Deencapsulation.setField(starosAgent, "serviceId", "2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to get file store, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.getFileStoreByName("test-fsname"));
    }

    @Test
    public void testGetFileStore() throws StarClientException, DdlException {
        S3FileStoreInfo s3FsInfo = S3FileStoreInfo.newBuilder()
                .setRegion("region").setEndpoint("endpoint").build();
        FileStoreInfo fsInfo = FileStoreInfo.newBuilder().setFsKey("test-fskey")
                .setFsName("test-fsname").setFsType(FileStoreType.S3).setS3FsInfo(s3FsInfo).build();
        new Expectations() {
            {
                client.getFileStore("test-fskey", "1");
                result = fsInfo;
                minTimes = 0;

                client.getFileStore("test-fskey", "2");
                result = new StarClientException(StatusCode.INVALID_ARGUMENT, "mocked exception");
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assertions.assertEquals("test-fskey", starosAgent.getFileStore("test-fskey").getFsKey());

        Deencapsulation.setField(starosAgent, "serviceId", "2");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Failed to get file store, error: INVALID_ARGUMENT:mocked exception",
                () -> starosAgent.getFileStore("test-fskey"));
    }

    @Test
    public void testListDefaultWorkerGroupIpPort() throws StarClientException, DdlException, StarRocksException {
        new MockUp<StarClient>() {
            @Mock
            public List<WorkerGroupDetailInfo> listWorkerGroup(String serviceId, List<Long> groupIds, boolean include) {
                long workerId0 = 10000L;
                WorkerInfo worker0 = newWorkerInfo(workerId0, "127.0.0.1:8090", 9050, 9060, 8040, 8060, 10);
                long workerId1 = 10001L;
                WorkerInfo worker1 = newWorkerInfo(workerId1, "127.0.0.2:8091", 9051, 9061, 8041, 8061, 10);
                WorkerGroupDetailInfo group = WorkerGroupDetailInfo.newBuilder().addWorkersInfo(worker0)
                        .addWorkersInfo(worker1).build();
                return Lists.newArrayList(group);
            }
        };
        List<String> addresses = starosAgent.listWorkerGroupIpPort(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
        Assertions.assertEquals("127.0.0.1:8090", addresses.get(0));
        Assertions.assertEquals("127.0.0.2:8091", addresses.get(1));
    }

    @Test
    public void testCreateWorkerGroup() throws StarClientException, DdlException, StarRocksException {
        new MockUp<StarClient>() {
            @Mock
            public WorkerGroupDetailInfo createWorkerGroup(String serviceId, String owner, WorkerGroupSpec spec,
                                                           Map<String, String> labels, Map<String, String> properties,
                                                           int replicaNumber, ReplicationType replicationType,
                                                           WarmupLevel warmupLevel) throws StarClientException {
                return WorkerGroupDetailInfo.newBuilder().build();
            }
        };
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.createWorkerGroup("size");
        starosAgent.createWorkerGroup("size", 1);
        starosAgent.createWorkerGroup("size", 1, ReplicationType.SYNC);
        starosAgent.createWorkerGroup("size", 1, ReplicationType.ASYNC, WarmupLevel.WARMUP_META);
        starosAgent.createWorkerGroup("size", 2, ReplicationType.NO_SET, WarmupLevel.WARMUP_NOT_SET,
                Collections.emptyMap());
    }

    @Test
    public void testUpdateWorkerGroup() throws StarClientException, DdlException, StarRocksException {
        new MockUp<StarClient>() {
            @Mock
            public WorkerGroupDetailInfo updateWorkerGroup(String serviceId, long groupId, Map<String, String> labels,
                                                           Map<String, String> properties, int replicaNumber,
                                                           ReplicationType replicationType, WarmupLevel warmupLevel)
                    throws StarClientException {
                return WorkerGroupDetailInfo.newBuilder().build();
            }
        };
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        starosAgent.updateWorkerGroup(123, 1);
        starosAgent.updateWorkerGroup(123, 1, ReplicationType.SYNC);
        starosAgent.updateWorkerGroup(123, 1, ReplicationType.ASYNC, WarmupLevel.WARMUP_META);
    }

    @Test
    public void testListShard() throws StarClientException, DdlException {
        ShardInfo shardInfo = ShardInfo.newBuilder().setShardId(1000L).build();
        List<List<ShardInfo>> infos = new ArrayList<>();
        infos.add(Lists.newArrayList(shardInfo));
        new Expectations() {
            {
                client.listShard("1", Lists.newArrayList(999L), StarOSAgent.DEFAULT_WORKER_GROUP_ID, true);
                result = infos;
                minTimes = 1;
                maxTimes = 1;
            }
        };
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        List<Long> ids = starosAgent.listShard(999L);
        Assertions.assertEquals(1, ids.size());
        Assertions.assertEquals((Long) 1000L, (Long) ids.get(0));
    }

    @Test
    public void testUpdateWorkerGroupExcepted() throws StarClientException {
        long workerGroupId = 10086;
        Map<String, String> properties = new HashMap<>();
        properties.put("A", "a");
        StarClientException expectedException = new StarClientException(StarStatus.newBuilder()
                .setStatusCode(StatusCode.INTERNAL)
                .setErrorMsg("Injected internal error from unit test: testUpdateWorkerGroupExcepted")
                .build());
        new Expectations() {
            {
                client.updateWorkerGroup("1", workerGroupId, null, properties, 0, ReplicationType.NO_SET,
                        WarmupLevel.WARMUP_NOT_SET);
                result = expectedException;
                result = null;
                minTimes = 2;
                maxTimes = 2;
            }
        };

        Deencapsulation.setField(starosAgent, "serviceId", "1");
        // first call, exception thrown
        Assertions.assertThrows(DdlException.class, () -> starosAgent.updateWorkerGroup(workerGroupId, properties));
        // second call, no exception
        ExceptionChecker.expectThrowsNoException(() -> starosAgent.updateWorkerGroup(workerGroupId, properties));
    }

    @Test
    public void testGetWorkerGroupInfoNormal() throws StarClientException {
        long workerGroupId = 10086;
        Map<String, String> properties = new HashMap<>();
        properties.put("A", "a");
        WorkerGroupDetailInfo expectedInfo = WorkerGroupDetailInfo.newBuilder()
                .putAllProperties(properties)
                .setGroupId(workerGroupId)
                .build();

        new Expectations() {
            {
                client.listWorkerGroup("1", Lists.newArrayList(workerGroupId), false);
                result = expectedInfo;
                minTimes = 1;
                maxTimes = 1;
            }
        };
        Deencapsulation.setField(starosAgent, "serviceId", "1");

        List<WorkerGroupDetailInfo> infos = new ArrayList<>();
        ExceptionChecker.expectThrowsNoException(() -> {
            WorkerGroupDetailInfo info = starosAgent.getWorkerGroupInfo(workerGroupId);
            infos.add(info);
        });
        Assertions.assertEquals(1, infos.size());
        Assertions.assertEquals(expectedInfo.toString(), infos.get(0).toString());
    }

    @Test
    public void testGetWorkerGroupInfoExcepted() throws StarClientException {
        long workerGroupId = 10086;
        StarClientException expectedException = new StarClientException(StarStatus.newBuilder()
                .setStatusCode(StatusCode.INTERNAL)
                .setErrorMsg("Injected internal error from unit test: testGetWorkerGroupInfoExcepted")
                .build());
        new Expectations() {
            {
                client.listWorkerGroup("1", Lists.newArrayList(workerGroupId), false);
                result = expectedException;
                minTimes = 1;
                maxTimes = 1;
            }
        };
        Deencapsulation.setField(starosAgent, "serviceId", "1");
        Assertions.assertThrows(DdlException.class, () -> starosAgent.getWorkerGroupInfo(workerGroupId));
    }
}
