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

#include "exprs/table_function/java_udtf_function.h"

#include <memory>
#include <utility>

#include "column/array_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "exprs/function_context.h"
#include "exprs/table_function/table_function.h"
#include "gutil/casts.h"
#include "jni.h"
#include "runtime/types.h"
#include "runtime/user_function_cache.h"
#include "udf/java/java_data_converter.h"
#include "udf/java/java_udf.h"
#include "udf/java/utils.h"
#include "util/defer_op.h"

namespace starrocks {

const TableFunction* getJavaUDTFFunction() {
    static JavaUDTFFunction java_table_function;
    return &java_table_function;
}

class JavaUDTFState : public TableFunctionState {
public:
    JavaUDTFState(std::string libpath, std::string symbol, std::vector<TypeDescriptor> type_desc, const TTypeDesc& desc)
            : _libpath(std::move(libpath)),
              _symbol(std::move(symbol)),
              _arg_type_descs(std::move(type_desc)),
              _ret_type(TypeDescriptor::from_thrift(desc)) {}
    ~JavaUDTFState() override = default;

    Status open();
    void close();

    const TypeDescriptor& type_desc() { return _ret_type; }
    JavaMethodDescriptor* method_process() { return _process.get(); }
    const std::vector<TypeDescriptor>& arg_type_descs() const { return _arg_type_descs; }
    jclass get_udtf_clazz() { return _udtf_class.clazz(); }
    jobject handle() { return _udtf_handle.handle(); }

private:
    std::string _libpath;
    std::string _symbol;

    std::unique_ptr<ClassLoader> _class_loader;
    std::unique_ptr<ClassAnalyzer> _analyzer;
    JVMClass _udtf_class = nullptr;
    JavaGlobalRef _udtf_handle = nullptr;
    std::unique_ptr<JavaMethodDescriptor> _process;
    std::vector<TypeDescriptor> _arg_type_descs;
    TypeDescriptor _ret_type;
};

Status JavaUDTFState::open() {
    RETURN_IF_ERROR(detect_java_runtime());
    _class_loader = std::make_unique<ClassLoader>(std::move(_libpath));
    RETURN_IF_ERROR(_class_loader->init());
    _analyzer = std::make_unique<ClassAnalyzer>();

    ASSIGN_OR_RETURN(_udtf_class, _class_loader->getClass(_symbol));
    ASSIGN_OR_RETURN(_udtf_handle, _udtf_class.newInstance());

    auto* analyzer = _analyzer.get();
    auto add_method = [&](const std::string& name, jclass clazz, std::unique_ptr<JavaMethodDescriptor>* res) {
        std::string method_name = name;
        std::string signature;
        std::vector<MethodTypeDescriptor> mtdesc;
        RETURN_IF_ERROR(analyzer->get_signature(clazz, method_name, &signature));
        RETURN_IF_ERROR(analyzer->get_udaf_method_desc(signature, &mtdesc));
        *res = std::make_unique<JavaMethodDescriptor>();
        (*res)->name = std::move(method_name);
        (*res)->signature = std::move(signature);
        (*res)->method_desc = std::move(mtdesc);
        return Status::OK();
    };
    RETURN_IF_ERROR(add_method("process", _udtf_class.clazz(), &_process));

    return Status::OK();
}

Status JavaUDTFFunction::init(const TFunction& fn, TableFunctionState** state) const {
    std::string libpath;
    auto instance = UserFunctionCache::instance();
    RETURN_IF_ERROR(instance->get_libpath(fn.fid, fn.hdfs_location, fn.checksum, TFunctionBinaryType::SRJAR, &libpath));
    // Now we only support one return types
    std::vector<TypeDescriptor> arg_typedescs;
    for (auto& type : fn.arg_types) {
        arg_typedescs.push_back(TypeDescriptor::from_thrift(type));
    }
    *state = new JavaUDTFState(std::move(libpath), fn.table_fn.symbol, arg_typedescs, fn.table_fn.ret_types[0]);
    return Status::OK();
}

Status JavaUDTFFunction::prepare(TableFunctionState* state) const {
    // Nothing to do
    return Status::OK();
}

Status JavaUDTFFunction::open(RuntimeState* runtime_state, TableFunctionState* state) const {
    auto open_status = [state]() {
        RETURN_IF_ERROR(down_cast<JavaUDTFState*>(state)->open());
        return Status::OK();
    };
    auto promise = call_function_in_pthread(runtime_state, open_status);
    RETURN_IF_ERROR(promise->get_future().get());
    return Status::OK();
}

Status JavaUDTFFunction::close(RuntimeState* runtime_state, TableFunctionState* state) const {
    auto promise = call_function_in_pthread(runtime_state, [state]() {
        delete state;
        return Status::OK();
    });
    RETURN_IF_ERROR(promise->get_future().get());
    return Status::OK();
}

std::pair<Columns, UInt32Column::Ptr> JavaUDTFFunction::process(RuntimeState* runtime_state,
                                                                TableFunctionState* state) const {
    Columns res;
    const Columns& cols = state->get_columns();
    auto* stateUDTF = down_cast<JavaUDTFState*>(state);

    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    jmethodID methodID = env->GetMethodID(stateUDTF->get_udtf_clazz(), stateUDTF->method_process()->name.c_str(),
                                          stateUDTF->method_process()->signature.c_str());

    std::vector<jvalue> call_stack;
    std::vector<jobject> rets;
    size_t num_rows = cols[0]->size();
    size_t num_cols = cols.size();
    state->set_processed_rows(num_rows);

    call_stack.reserve(num_cols);
    rets.resize(num_rows);

    // reserve 16 local refs
    DeferOp defer = DeferOp([&]() {
        // clean up arrays
        env->PopLocalFrame(nullptr);
    });
    env->PushLocalFrame(num_cols * num_rows + 16);

    for (int i = 0; i < num_rows; ++i) {
        DeferOp defer = DeferOp([&]() {
            for (int j = 0; j < num_cols; ++j) {
                release_jvalue(stateUDTF->method_process()->method_desc[j + 1].is_box, call_stack[j]);
            }
            call_stack.clear();
        });

        for (int j = 0; j < num_cols; ++j) {
            auto method_type = stateUDTF->method_process()->method_desc[j + 1];
            auto val_st = cast_to_jvalue(stateUDTF->arg_type_descs()[j], method_type.is_box, cols[j].get(), i);
            if (!val_st.ok()) {
                stateUDTF->set_status(val_st.status());
                return {};
            }
            call_stack.push_back(val_st.value());
        }

        rets[i] = env->CallObjectMethodA(stateUDTF->handle(), methodID, call_stack.data());

        if (auto jthr = helper.getEnv()->ExceptionOccurred(); jthr != nullptr) {
            std::string err = fmt::format("execute UDF Function meet Exception:{}", helper.dumpExceptionString(jthr));
            LOG(WARNING) << err;
            helper.getEnv()->ExceptionClear();
            state->set_status(Status::InternalError(err));
            return std::make_pair(Columns{}, nullptr);
        }
    }

    // Build Return Type
    auto offsets_col = UInt32Column::create();
    auto& offsets = offsets_col->get_data();
    offsets.resize(num_rows + 1);

    auto col = ColumnHelper::create_column(stateUDTF->type_desc(), true);
    col->reserve(num_rows);

    for (int i = 0; i < num_rows; ++i) {
        int len = rets[i] != nullptr ? env->GetArrayLength((jarray)rets[i]) : 0;
        offsets[i + 1] = offsets[i] + len;
        // update for col
        for (int j = 0; j < len; ++j) {
            jobject vi = env->GetObjectArrayElement((jobjectArray)rets[i], j);
            LOCAL_REF_GUARD_ENV(env, vi);
            auto st = check_type_matched(stateUDTF->type_desc(), vi);
            if (UNLIKELY(!st.ok())) {
                state->set_status(st);
                return {};
            }
            auto res = append_jvalue(stateUDTF->type_desc(), true, col.get(), {.l = vi});
            if (UNLIKELY(!res.ok())) {
                state->set_status(Status::InternalError(res.to_string()));
                return {};
            }
        }
    }

    res.emplace_back(std::move(col));

    // TODO: add error msg to Function State
    if (auto jthr = helper.getEnv()->ExceptionOccurred(); jthr != nullptr) {
        std::string err = fmt::format("execute UDF Function meet Exception:{}", helper.dumpExceptionString(jthr));
        LOG(WARNING) << err;
        helper.getEnv()->ExceptionClear();
    }

    return std::make_pair(std::move(res), std::move(offsets_col));
}

} // namespace starrocks
