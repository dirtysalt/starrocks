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


package com.starrocks.rpc;

import com.baidu.jprotobuf.pbrpc.client.ProtobufRpcProxy;
import com.baidu.jprotobuf.pbrpc.transport.RpcClient;
import com.baidu.jprotobuf.pbrpc.transport.RpcClientOptions;
import com.starrocks.common.Config;
import com.starrocks.common.util.DnsCache;
import com.starrocks.service.FrontendOptions;
import com.starrocks.thrift.TNetworkAddress;

import java.util.concurrent.ConcurrentHashMap;

public class BrpcProxy {
    private final RpcClient rpcClient;
    // TODO: Eviction
    private final ConcurrentHashMap<TNetworkAddress, PBackendService> backendServiceMap;
    private final ConcurrentHashMap<TNetworkAddress, LakeService> lakeServiceMap;

    public BrpcProxy() {
        final RpcClientOptions rpcOptions = new RpcClientOptions();
        // If false, different methods to a service endpoint use different connection pool,
        // which will create too many connections.
        // If true, all the methods to a service endpoint use the same connection pool.
        rpcOptions.setShareThreadPoolUnderEachProxy(true);
        rpcOptions.setShareChannelPool(true);
        rpcOptions.setMaxTotoal(Config.brpc_connection_pool_size);
        // After the RPC request sending, the connection will be closed,
        // if the number of total connections is greater than MaxIdleSize.
        // Therefore, MaxIdleSize shouldn't less than MaxTotal for the async requests.
        rpcOptions.setMaxIdleSize(Config.brpc_connection_pool_size);
        rpcOptions.setMaxWait(Config.brpc_idle_wait_max_time);
        rpcOptions.setJmxEnabled(true);
        rpcOptions.setReuseAddress(Config.brpc_reuse_addr);
        rpcOptions.setMinEvictableIdleTime(Config.brpc_min_evictable_idle_time_ms);
        rpcOptions.setShortConnection(Config.brpc_short_connection);
        rpcOptions.setInnerResuePool(Config.brpc_inner_reuse_pool);

        rpcClient = new RpcClient(rpcOptions);
        backendServiceMap = new ConcurrentHashMap<>();
        lakeServiceMap = new ConcurrentHashMap<>();
    }

    private static BrpcProxy getInstance() {
        return BrpcProxy.SingletonHolder.INSTANCE;
    }

    /**
     * Only used for pseudo cluster or unittest
     */
    public static void setInstance(BrpcProxy proxy) {
        BrpcProxy.SingletonHolder.INSTANCE = proxy;
    }

    public static TNetworkAddress convertToIpAddress(TNetworkAddress address) {
        if (!FrontendOptions.isUseFqdn()) {
            return address;
        }
        String ip = DnsCache.tryLookup(address.getHostname());
        return new TNetworkAddress(ip, address.getPort());
    }

    public static PBackendService getBackendService(TNetworkAddress address) {
        return getInstance().getBackendServiceImpl(address);
    }

    public static LakeService getLakeService(TNetworkAddress address) throws RpcException {
        return getInstance().getLakeServiceImpl(address);
    }

    public static LakeService getLakeService(String host, int port) throws RpcException {
        return getInstance().getLakeServiceImpl(new TNetworkAddress(host, port));
    }

    protected PBackendService getBackendServiceImpl(TNetworkAddress address) {
        TNetworkAddress cacheAddress = convertToIpAddress(address);
        return backendServiceMap.computeIfAbsent(cacheAddress, this::createBackendService);
    }

    protected LakeService getLakeServiceImpl(TNetworkAddress address) throws RpcException {
        try {
            TNetworkAddress cacheAddress = convertToIpAddress(address);
            return lakeServiceMap.computeIfAbsent(cacheAddress, this::createLakeService);
        } catch (Exception e) {
            throw new RpcException("fail to initialize the LakeService on node " + address.getHostname(), e);
        }
    }

    private PBackendService createBackendService(TNetworkAddress address) {
        ProtobufRpcProxy<PBackendService> proxy = new ProtobufRpcProxy<>(rpcClient, PBackendService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        return new PBackendServiceWithMetrics(proxy.proxy());
    }

    private LakeService createLakeService(TNetworkAddress address) {
        ProtobufRpcProxy<LakeService> proxy = new ProtobufRpcProxy<>(rpcClient, LakeService.class);
        proxy.setHost(address.getHostname());
        proxy.setPort(address.getPort());
        return new LakeServiceWithMetrics(proxy.proxy());
    }

    private static class SingletonHolder {
        private static BrpcProxy INSTANCE = new BrpcProxy();
    }
}
