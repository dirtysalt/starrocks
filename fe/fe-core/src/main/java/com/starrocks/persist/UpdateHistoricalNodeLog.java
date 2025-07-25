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

package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

public class UpdateHistoricalNodeLog implements Writable {
    @SerializedName(value = "warehouse")
    private String warehouse;

    @SerializedName(value = "warehouseId")
    private long warehouseId;

    @SerializedName(value = "workerGroupId")
    private long workerGroupId;

    @SerializedName(value = "updateTime")
    private long updateTime;

    @SerializedName(value = "backendIds")
    private List<Long> backendIds;

    @SerializedName(value = "computeNodeIds")
    private List<Long> computeNodeIds;

    public UpdateHistoricalNodeLog(long warehouseId, long workerGroupId, long updateTime, List<Long> backendIds,
                                   List<Long> computeNodeIds) {
        this.warehouseId = warehouseId;
        this.workerGroupId = workerGroupId;
        this.updateTime = updateTime;
        this.backendIds = backendIds;
        this.computeNodeIds = computeNodeIds;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public long getWarehouseId() {
        return warehouseId;
    }

    public long getWorkerGroupId() {
        return workerGroupId;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    public List<Long> getBackendIds() {
        return backendIds;
    }

    public List<Long> getComputeNodeIds() {
        return computeNodeIds;
    }

    public static UpdateHistoricalNodeLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), UpdateHistoricalNodeLog.class);
    }
}