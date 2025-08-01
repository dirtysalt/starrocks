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

package com.starrocks.qe.scheduler;

import com.google.api.client.util.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.profile.Timer;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ExecuteExceptionHandler;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.qe.scheduler.slot.DeployState;
import com.starrocks.rpc.RpcException;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.starrocks.qe.scheduler.dag.FragmentInstanceExecState.DeploymentResult;

/**
 * The utility class to deploy fragment instances to workers.
 */
public class Deployer {
    private static final Logger LOG = LogManager.getLogger(Deployer.class);
    private static final ThreadPoolExecutor EXECUTOR;

    static {
        int threadPoolSize = Math.max(ThreadPoolManager.cpuCores(), Config.deploy_serialization_thread_pool_size);
        int threadPoolQueueSize = Math.max(threadPoolSize * 2, Config.deploy_serialization_queue_size);
        EXECUTOR = ThreadPoolManager.newDaemonThreadPool(1, threadPoolSize, 60, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(threadPoolQueueSize), new ThreadPoolExecutor.AbortPolicy(),
                "deployer", true);
        ThreadPoolManager.registerAllThreadPoolMetric();
    }

    private final ConnectContext context;
    private final JobSpec jobSpec;
    private final ExecutionDAG executionDAG;

    private final TFragmentInstanceFactory tFragmentInstanceFactory;
    private final TDescriptorTable emptyDescTable;
    private final long deliveryTimeoutMs;
    private final boolean enablePlanSerializeConcurrently;

    private final FailureHandler failureHandler;
    private final boolean needDeploy;

    private final Set<Long> deployedWorkerIds = Sets.newHashSet();

    public Deployer(ConnectContext context,
                    JobSpec jobSpec,
                    ExecutionDAG executionDAG,
                    TNetworkAddress coordAddress,
                    FailureHandler failureHandler,
                    boolean needDeploy) {
        this.context = context;
        this.jobSpec = jobSpec;
        this.executionDAG = executionDAG;

        this.tFragmentInstanceFactory = new TFragmentInstanceFactory(context, jobSpec, executionDAG, coordAddress);
        this.emptyDescTable = new TDescriptorTable()
                .setIs_cached(true)
                .setTupleDescriptors(Collections.emptyList());

        TQueryOptions queryOptions = jobSpec.getQueryOptions();
        this.deliveryTimeoutMs = Math.min(queryOptions.query_timeout, queryOptions.query_delivery_timeout) * 1000L;

        this.failureHandler = failureHandler;
        this.needDeploy = needDeploy;
        this.enablePlanSerializeConcurrently = context.getSessionVariable().getEnablePlanSerializeConcurrently();
    }

    public DeployState createFragmentExecStates(List<ExecutionFragment> concurrentFragments) {
        final DeployState deployState = new DeployState();
        concurrentFragments.forEach(fragment ->
                this.createFragmentInstanceExecStates(fragment, deployState.getThreeStageExecutionsToDeploy()));
        return deployState;
    }

    public void deployFragments(DeployState deployState)
            throws RpcException, StarRocksException {

        if (!needDeploy) {
            return;
        }

        final List<List<FragmentInstanceExecState>> threeStageExecutionsToDeploy =
                deployState.getThreeStageExecutionsToDeploy();

        if (enablePlanSerializeConcurrently) {
            try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeploySerializeConcurrencyTime")) {
                int count = threeStageExecutionsToDeploy.stream().mapToInt(List::size).sum();
                List<Future<?>> futures = new ArrayList<>(count + 1);
                for (List<FragmentInstanceExecState> execStates : threeStageExecutionsToDeploy) {
                    for (FragmentInstanceExecState execState : execStates) {
                        try {
                            Future<?> f = EXECUTOR.submit(execState::serializeRequest);
                            futures.add(f);
                        } catch (RejectedExecutionException e) {
                            // If the thread pool is full, we will serialize the request in the current thread.
                        }
                    }
                }
                for (Future<?> future : futures) {
                    try {
                        future.get(2, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        LOG.warn("Slow serialize request, query: {}", DebugUtil.printId(context.getQueryId()));
                    }
                }
                for (Future<?> future : futures) {
                    if (!future.isDone()) {
                        future.get();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                LOG.warn("Error serialize request during deployFragments", e);
                throw new StarRocksException(e);
            }
        }

        for (List<FragmentInstanceExecState> executions : threeStageExecutionsToDeploy) {
            try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeployStageByStageTime")) {
                executions.forEach(FragmentInstanceExecState::deployAsync);
            }
            try (Timer ignored = Tracers.watchScope(Tracers.Module.SCHEDULER, "DeployWaitTime")) {
                waitForDeploymentCompletion(executions);
            }
        }
    }

    public interface FailureHandler {
        void apply(Status status, FragmentInstanceExecState execution, Throwable failure) throws RpcException,
                StarRocksException;
    }

    private void createFragmentInstanceExecStates(ExecutionFragment fragment,
                                                  List<List<FragmentInstanceExecState>> threeStageExecutionsToDeploy) {
        Preconditions.checkState(!fragment.getInstances().isEmpty());

        // This is a load process, and it is the first fragment.
        // we should add all BackendExecState of this fragment to needCheckBackendExecStates,
        // so that we can check these backends' state when joining this Coordinator
        boolean needCheckExecutionState = jobSpec.isLoadType() && fragment.getFragmentIndex() == 0;
        boolean isEnablePipeline = jobSpec.isEnablePipeline();
        // if pipeline is enable and current fragment contain olap table sink, in fe we will
        // calculate the number of all tablet sinks in advance and assign them to each fragment instance
        boolean enablePipelineTableSinkDop = isEnablePipeline && fragment.getPlanFragment().hasTableSink();

        List<List<FragmentInstance>> threeStageInstancesToDeploy = ImmutableList.of(
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>());

        // Fragment Instance carrying runtime filter params for runtime filter coordinator
        // must be delivered at first.
        Map<Boolean, List<FragmentInstance>> instanceSplits =
                fragment.getInstances().stream().collect(
                        Collectors.partitioningBy(instance -> instance.getExecFragment().isRuntimeFilterCoordinator()));
        // stage 0 holds the instance carrying runtime filter params that used to initialize
        // global runtime filter coordinator if exists.
        threeStageInstancesToDeploy.get(0).addAll(instanceSplits.get(true));

        List<FragmentInstance> restInstances = instanceSplits.get(false);
        if (!isEnablePipeline) {
            threeStageInstancesToDeploy.get(1).addAll(restInstances);
        } else {
            threeStageInstancesToDeploy.get(0).forEach(instance -> deployedWorkerIds.add(instance.getWorkerId()));
            restInstances.forEach(instance -> {
                if (deployedWorkerIds.contains(instance.getWorkerId())) {
                    threeStageInstancesToDeploy.get(2).add(instance);
                } else {
                    deployedWorkerIds.add(instance.getWorkerId());
                    threeStageInstancesToDeploy.get(1).add(instance);
                }
            });
        }

        int totalTableSinkDop = 0;
        if (enablePipelineTableSinkDop) {
            totalTableSinkDop = threeStageInstancesToDeploy.stream()
                    .flatMap(Collection::stream)
                    .mapToInt(FragmentInstance::getTableSinkDop)
                    .sum();
        }
        Preconditions.checkState(totalTableSinkDop >= 0,
                "tableSinkTotalDop = %d should be >= 0", totalTableSinkDop);

        int accTabletSinkDop = 0;
        for (int stageIndex = 0; stageIndex < threeStageInstancesToDeploy.size(); stageIndex++) {
            List<FragmentInstance> stageInstances = threeStageInstancesToDeploy.get(stageIndex);
            if (stageInstances.isEmpty()) {
                continue;
            }

            TDescriptorTable curDescTable;
            if (stageIndex < 2) {
                curDescTable = jobSpec.getDescTable();
            } else {
                curDescTable = emptyDescTable;
            }

            for (FragmentInstance instance : stageInstances) {
                TExecPlanFragmentParams request =
                        tFragmentInstanceFactory.create(instance, curDescTable, accTabletSinkDop, totalTableSinkDop);
                if (enablePipelineTableSinkDop) {
                    accTabletSinkDop += instance.getTableSinkDop();
                }

                FragmentInstanceExecState execution = FragmentInstanceExecState.createExecution(
                        jobSpec,
                        instance.getFragmentId(),
                        fragment.getFragmentIndex(),
                        request,
                        instance.getWorker());
                execution.setFragmentInstance(instance);

                threeStageExecutionsToDeploy.get(stageIndex).add(execution);

                executionDAG.addExecution(execution);

                if (needCheckExecutionState) {
                    executionDAG.addNeedCheckExecution(execution);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("add need check backend {} for fragment, {} job: {}",
                                execution.getWorker().getId(),
                                fragment.getFragmentId().asInt(), jobSpec.getLoadJobId());
                    }
                }
            }
        }
    }

    private void waitForDeploymentCompletion(List<FragmentInstanceExecState> executions) throws RpcException,
            StarRocksException {
        if (executions.isEmpty()) {
            return;
        }
        DeploymentResult firstErrResult = null;
        FragmentInstanceExecState firstErrExecution = null;
        for (FragmentInstanceExecState execution : executions) {
            DeploymentResult res = execution.waitForDeploymentCompletion(deliveryTimeoutMs);
            if (TStatusCode.OK == res.getStatusCode()) {
                continue;
            }

            // Handle error results and cancel fragment instances, excluding TIMEOUT errors,
            // until all the delivered fragment instances are completed.
            // Otherwise, the cancellation RPC may arrive at BE before the delivery fragment instance RPC,
            // causing the instances to become stale and only able to be released after a timeout.
            if (firstErrResult == null) {
                firstErrResult = res;
                firstErrExecution = execution;
            } else if (firstErrResult.getStatusCode() == TStatusCode.CANCELLED &&
                    ExecuteExceptionHandler.isRetryableStatus(res.getStatusCode())) {
                // If the first error is cancelled and the subsequent error is retryable, we store the latter to give a chance
                // to retry this query.
                firstErrResult = res;
                firstErrExecution = execution;
            }
            if (TStatusCode.TIMEOUT == res.getStatusCode()) {
                break;
            }
        }

        if (firstErrResult != null) {
            failureHandler.apply(firstErrResult.getStatus(), firstErrExecution, firstErrResult.getFailure());
        }
    }

    public TExecPlanFragmentParams createIncrementalScanRangesRequest(FragmentInstance instance) {
        return tFragmentInstanceFactory.createIncrementalScanRanges(instance);
    }
}

