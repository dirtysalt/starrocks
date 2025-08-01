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

#include "exprs/time_functions.h"

#include <cctz/time_zone.h>
#include <column/column_view/column_view.h>
#include <libdivide.h>

#include <algorithm>
#include <string_view>
#include <unordered_map>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/binary_function.h"
#include "exprs/unary_function.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "types/date_value.h"

namespace starrocks {
// index as day of week(1: Sunday, 2: Monday....), value as distance of this day and first day(Monday) of this week.
static int day_to_first[8] = {0 /*never use*/, 6, 0, 1, 2, 3, 4, 5};

// avoid format function OOM, the value just based on experience
const static int DEFAULT_DATE_FORMAT_LIMIT = 100;

#define DEFINE_TIME_UNARY_FN(NAME, TYPE, RESULT_TYPE)                                                         \
    StatusOr<ColumnPtr> TimeFunctions::NAME(FunctionContext* context, const starrocks::Columns& columns) {    \
        return VectorizedStrictUnaryFunction<NAME##Impl>::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0)); \
    }

#define DEFINE_TIME_STRING_UNARY_FN(NAME, TYPE, RESULT_TYPE)                                                        \
    StatusOr<ColumnPtr> TimeFunctions::NAME(FunctionContext* context, const starrocks::Columns& columns) {          \
        return VectorizedStringStrictUnaryFunction<NAME##Impl>::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0)); \
    }

#define DEFINE_TIME_UNARY_FN_WITH_IMPL(NAME, TYPE, RESULT_TYPE, FN) \
    DEFINE_UNARY_FN(NAME##Impl, FN);                                \
    DEFINE_TIME_UNARY_FN(NAME, TYPE, RESULT_TYPE);

#define DEFINE_TIME_BINARY_FN(NAME, LTYPE, RTYPE, RESULT_TYPE)                                                         \
    StatusOr<ColumnPtr> TimeFunctions::NAME(FunctionContext* context, const starrocks::Columns& columns) {             \
        return VectorizedStrictBinaryFunction<NAME##Impl>::evaluate<LTYPE, RTYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(0),  \
                                                                                               VECTORIZED_FN_ARGS(1)); \
    }

#define DEFINE_TIME_BINARY_FN_WITH_IMPL(NAME, LTYPE, RTYPE, RESULT_TYPE, FN) \
    DEFINE_BINARY_FUNCTION(NAME##Impl, FN);                                  \
    DEFINE_TIME_BINARY_FN(NAME, LTYPE, RTYPE, RESULT_TYPE);

#define DEFINE_TIME_UNARY_FN_EXTEND(NAME, TYPE, RESULT_TYPE, IDX)                                               \
    StatusOr<ColumnPtr> TimeFunctions::NAME(FunctionContext* context, const starrocks::Columns& columns) {      \
        return VectorizedStrictUnaryFunction<NAME##Impl>::evaluate<TYPE, RESULT_TYPE>(VECTORIZED_FN_ARGS(IDX)); \
    }

template <LogicalType Type>
ColumnPtr date_valid(const ColumnPtr& v1) {
    if (v1->only_null()) {
        return v1;
    }

    if (v1->is_constant()) {
        auto value = ColumnHelper::get_const_value<Type>(v1);
        if (value.is_valid_non_strict()) {
            return v1;
        } else {
            return ColumnHelper::create_const_null_column(v1->size());
        }
    } else if (v1->is_nullable()) {
        auto v = ColumnHelper::as_column<NullableColumn>(v1);
        auto& nulls = v->null_column()->get_data();
        auto& values = ColumnHelper::cast_to_raw<Type>(v->data_column())->get_data();

        auto null_column = NullColumn::create();
        null_column->resize(v1->size());
        auto& null_result = null_column->get_data();

        int size = v->size();
        for (int i = 0; i < size; ++i) {
            // if null or is invalid
            null_result[i] = nulls[i] | (!values[i].is_valid_non_strict());
        }

        return NullableColumn::create(v->data_column(), std::move(null_column));
    } else {
        auto null_column = NullColumn::create();
        null_column->resize(v1->size());
        auto& nulls = null_column->get_data();
        auto& values = ColumnHelper::cast_to_raw<Type>(v1)->get_data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            nulls[i] = (!values[i].is_valid_non_strict());
        }

        return NullableColumn::create(v1, std::move(null_column));
    }
}

#define DEFINE_TIME_CALC_FN(NAME, LTYPE, RTYPE, RESULT_TYPE)                                               \
    StatusOr<ColumnPtr> TimeFunctions::NAME(FunctionContext* context, const starrocks::Columns& columns) { \
        auto p = VectorizedStrictBinaryFunction<NAME##Impl>::evaluate<LTYPE, RTYPE, RESULT_TYPE>(          \
                VECTORIZED_FN_ARGS(0), VECTORIZED_FN_ARGS(1));                                             \
        return date_valid<RESULT_TYPE>(p);                                                                 \
    }

Status TimeFunctions::convert_tz_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || context->get_num_args() != 3 ||
        context->get_arg_type(1)->type != TYPE_VARCHAR || context->get_arg_type(2)->type != TYPE_VARCHAR ||
        !context->is_constant_column(1) || !context->is_constant_column(2)) {
        return Status::OK();
    }

    auto* ctc = new ConvertTzCtx();
    context->set_function_state(scope, ctc);

    // find from timezone
    auto from = context->get_constant_column(1);
    if (from->only_null()) {
        ctc->is_valid = false;
        return Status::OK();
    }

    auto from_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(from);
    if (!TimezoneUtils::find_cctz_time_zone(std::string_view(from_value), ctc->from_tz)) {
        ctc->is_valid = false;
        return Status::OK();
    }

    // find to timezone
    auto to = context->get_constant_column(2);
    if (to->only_null()) {
        ctc->is_valid = false;
        return Status::OK();
    }

    auto to_value = ColumnHelper::get_const_value<TYPE_VARCHAR>(to);
    if (!TimezoneUtils::find_cctz_time_zone(std::string_view(to_value), ctc->to_tz)) {
        ctc->is_valid = false;
        return Status::OK();
    }

    ctc->is_valid = true;
    return Status::OK();
}

Status TimeFunctions::convert_tz_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* ctc = reinterpret_cast<ConvertTzCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (ctc != nullptr) {
            delete ctc;
        }
    }

    return Status::OK();
}

StatusOr<ColumnPtr> TimeFunctions::convert_tz_general(FunctionContext* context, const Columns& columns) {
    auto time_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);
    auto from_str = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    auto to_str = ColumnViewer<TYPE_VARCHAR>(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATETIME> result(size);
    TimezoneHsScan timezone_hsscan;
    RETURN_IF_ERROR(timezone_hsscan.compile());
    for (int row = 0; row < size; ++row) {
        if (time_viewer.is_null(row) || from_str.is_null(row) || to_str.is_null(row)) {
            result.append_null();
            continue;
        }

        auto datetime_value = time_viewer.value(row);
        auto from_format = from_str.value(row);
        auto to_format = to_str.value(row);

        int year, month, day, hour, minute, second, usec;
        datetime_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
        DateTimeValue ts_value(TIME_DATETIME, year, month, day, hour, minute, second, usec);

        cctz::time_zone ctz;
        int64_t timestamp;
        int64_t offset;
        if (TimezoneUtils::timezone_offsets(from_format, to_format, &offset)) {
            TimestampValue ts = TimestampValue::create(year, month, day, hour, minute, second, usec);
            ts.from_unix_second(ts.to_unix_second() + offset, usec);
            result.append(ts);
            continue;
        }

        if (!ts_value.from_cctz_timezone(timezone_hsscan, from_format, ctz) ||
            !ts_value.unix_timestamp(&timestamp, ctz)) {
            result.append_null();
            continue;
        }

        DateTimeValue ts_value2;
        if (!ts_value2.from_cctz_timezone(timezone_hsscan, to_format, ctz) ||
            !ts_value2.from_unixtime(timestamp, ts_value.microsecond(), ctz)) {
            result.append_null();
            continue;
        }
        TimestampValue ts;
        ts.from_timestamp(ts_value2.year(), ts_value2.month(), ts_value2.day(), ts_value2.hour(), ts_value2.minute(),
                          ts_value2.second(), ts_value2.microsecond());
        result.append(ts);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::convert_tz_const(FunctionContext* context, const Columns& columns,
                                                    const cctz::time_zone& from, const cctz::time_zone& to) {
    auto time_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATETIME> result(size);
    for (int row = 0; row < size; ++row) {
        if (time_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto datetime_value = time_viewer.value(row);

        int year, month, day, hour, minute, second, usec;
        datetime_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
        DateTimeValue ts_value(TIME_DATETIME, year, month, day, hour, minute, second, usec);

        int64_t timestamp;
        // TODO find a better approach to replace datetime_value.unix_timestamp
        if (!ts_value.unix_timestamp(&timestamp, from)) {
            result.append_null();
            continue;
        }
        DateTimeValue ts_value2;
        // TODO find a better approach to replace datetime_value.from_unixtime
        if (!ts_value2.from_unixtime(timestamp, ts_value.microsecond(), to)) {
            result.append_null();
            continue;
        }
        TimestampValue ts;
        ts.from_timestamp(ts_value2.year(), ts_value2.month(), ts_value2.day(), ts_value2.hour(), ts_value2.minute(),
                          ts_value2.second(), ts_value2.microsecond());
        result.append(ts);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::convert_tz(FunctionContext* context, const Columns& columns) {
    auto* ctc = reinterpret_cast<ConvertTzCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (ctc == nullptr) {
        return convert_tz_general(context, columns);
    }

    if (!ctc->is_valid) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }

    return convert_tz_const(context, columns, ctc->from_tz, ctc->to_tz);
}

StatusOr<ColumnPtr> TimeFunctions::utc_timestamp(FunctionContext* context, const Columns& columns) {
    starrocks::RuntimeState* state = context->state();
    DateTimeValue dtv;
    if (dtv.from_unixtime(state->timestamp_ms() / 1000, "+00:00")) {
        TimestampValue ts;
        ts.from_timestamp(dtv.year(), dtv.month(), dtv.day(), dtv.hour(), dtv.minute(), dtv.second(), 0);
        return ColumnHelper::create_const_column<TYPE_DATETIME>(ts, 1);
    } else {
        return ColumnHelper::create_const_null_column(1);
    }
}

StatusOr<ColumnPtr> TimeFunctions::utc_time(FunctionContext* context, const Columns& columns) {
    starrocks::RuntimeState* state = context->state();
    DateTimeValue dtv;
    if (dtv.from_unixtime(state->timestamp_ms() / 1000, "+00:00")) {
        double seconds = dtv.hour() * 3600 + dtv.minute() * 60 + dtv.second();
        return ColumnHelper::create_const_column<TYPE_TIME>(seconds, 1);
    } else {
        return ColumnHelper::create_const_null_column(1);
    }
}

StatusOr<ColumnPtr> TimeFunctions::timestamp(FunctionContext* context, const Columns& columns) {
    return columns[0];
}

static const std::vector<int> NOW_PRECISION_FACTORS = {1000000, 100000, 10000, 1000, 100, 10, 1};

StatusOr<ColumnPtr> TimeFunctions::now(FunctionContext* context, const Columns& columns) {
    starrocks::RuntimeState* state = context->state();
    int64_t timestamp_us = state->timestamp_us();
    DateTimeValue dtv;
    if (!dtv.from_unixtime(timestamp_us / 1000000, state->timezone_obj())) {
        return ColumnHelper::create_const_null_column(1);
    }
    if (columns.empty()) {
        TimestampValue ts;
        ts.from_timestamp(dtv.year(), dtv.month(), dtv.day(), dtv.hour(), dtv.minute(), dtv.second(), 0);
        return ColumnHelper::create_const_column<TYPE_DATETIME>(ts, 1);
    }

    if (context->is_constant_column(1)) {
        auto col = context->get_constant_column(1);
        if (col->only_null()) {
            return Status::InvalidArgument("the precision of now function must between 0 and 6");
        }
        int precision = ColumnHelper::get_const_value<TYPE_INT>(col);
        if (precision < 0 || precision > 6) {
            return Status::InvalidArgument("the precision of now function must between 0 and 6");
        }
        TimestampValue ts;
        ts.from_timestamp(dtv.year(), dtv.month(), dtv.day(), dtv.hour(), dtv.minute(), dtv.second(),
                          timestamp_us % 1000000 / NOW_PRECISION_FACTORS[precision] * NOW_PRECISION_FACTORS[precision]);
        return ColumnHelper::create_const_column<TYPE_DATETIME>(ts, 1);
    }

    auto size = columns[0]->size();
    auto precision_viewer = ColumnViewer<TYPE_INT>(columns[0]);
    ColumnBuilder<TYPE_DATETIME> result(size);
    for (int row = 0; row < size; row++) {
        if (precision_viewer.is_null(row)) {
            return Status::InvalidArgument("the precision of now function must between 0 and 6");
        }
        int precision = precision_viewer.value(row);
        if (precision < 0 || precision > 6) {
            return Status::InvalidArgument("the precision of now function must between 0 and 6");
        }
        int factor = NOW_PRECISION_FACTORS[precision];
        TimestampValue ts;
        ts.from_timestamp(dtv.year(), dtv.month(), dtv.day(), dtv.hour(), dtv.minute(), dtv.second(),
                          timestamp_us % 1000000 / factor * factor);
        result.append(ts);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::curtime(FunctionContext* context, const Columns& columns) {
    starrocks::RuntimeState* state = context->state();
    DateTimeValue dtv;
    if (dtv.from_unixtime(state->timestamp_ms() / 1000, state->timezone())) {
        double seconds = dtv.hour() * 3600 + dtv.minute() * 60 + dtv.second();
        return ColumnHelper::create_const_column<TYPE_TIME>(seconds, 1);
    } else {
        return ColumnHelper::create_const_null_column(1);
    }
}

StatusOr<ColumnPtr> TimeFunctions::curdate(FunctionContext* context, const Columns& columns) {
    starrocks::RuntimeState* state = context->state();
    DateTimeValue dtv;
    if (dtv.from_unixtime(state->timestamp_ms() / 1000, state->timezone())) {
        DateValue dv;
        dv.from_date(dtv.year(), dtv.month(), dtv.day());
        return ColumnHelper::create_const_column<TYPE_DATE>(dv, 1);
    } else {
        return ColumnHelper::create_const_null_column(1);
    }
}

// year
DEFINE_UNARY_FN_WITH_IMPL(yearImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return y;
}

DEFINE_TIME_UNARY_FN(year, TYPE_DATETIME, TYPE_INT);

// year
// return type: INT16
DEFINE_UNARY_FN_WITH_IMPL(yearV2Impl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return y;
}

DEFINE_TIME_UNARY_FN(yearV2, TYPE_DATETIME, TYPE_SMALLINT);

DEFINE_UNARY_FN_WITH_IMPL(yearV3Impl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return y;
}

DEFINE_TIME_UNARY_FN(yearV3, TYPE_DATE, TYPE_SMALLINT);

// quarter
DEFINE_UNARY_FN_WITH_IMPL(quarterImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return (m - 1) / 3 + 1;
}
DEFINE_TIME_UNARY_FN(quarter, TYPE_DATETIME, TYPE_INT);

// month
DEFINE_UNARY_FN_WITH_IMPL(monthImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return m;
}
DEFINE_TIME_UNARY_FN(month, TYPE_DATETIME, TYPE_INT);

// month
// return type: INT8
DEFINE_UNARY_FN_WITH_IMPL(monthV2Impl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return m;
}
DEFINE_TIME_UNARY_FN(monthV2, TYPE_DATETIME, TYPE_TINYINT);

DEFINE_UNARY_FN_WITH_IMPL(monthV3Impl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return m;
}
DEFINE_TIME_UNARY_FN(monthV3, TYPE_DATE, TYPE_TINYINT);

// day
DEFINE_UNARY_FN_WITH_IMPL(dayImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return d;
}
DEFINE_TIME_UNARY_FN(day, TYPE_DATETIME, TYPE_INT);

// day
// return type: INT8
DEFINE_UNARY_FN_WITH_IMPL(dayV2Impl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return d;
}
DEFINE_TIME_UNARY_FN(dayV2, TYPE_DATETIME, TYPE_TINYINT);

DEFINE_UNARY_FN_WITH_IMPL(dayV3Impl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    return d;
}
DEFINE_TIME_UNARY_FN(dayV3, TYPE_DATE, TYPE_TINYINT);

// hour of the day
DEFINE_UNARY_FN_WITH_IMPL(hourImpl, v) {
    int hour1, mintue1, second1, usec1;
    v.to_time(&hour1, &mintue1, &second1, &usec1);
    return hour1;
}
DEFINE_TIME_UNARY_FN(hour, TYPE_DATETIME, TYPE_INT);

// hour of the day
DEFINE_UNARY_FN_WITH_IMPL(hourV2Impl, v) {
    int hour1, mintue1, second1, usec1;
    v.to_time(&hour1, &mintue1, &second1, &usec1);
    return hour1;
}
DEFINE_TIME_UNARY_FN(hourV2, TYPE_DATETIME, TYPE_TINYINT);

// minute of the hour
DEFINE_UNARY_FN_WITH_IMPL(minuteImpl, v) {
    int hour1, mintue1, second1, usec1;
    v.to_time(&hour1, &mintue1, &second1, &usec1);
    return mintue1;
}
DEFINE_TIME_UNARY_FN(minute, TYPE_DATETIME, TYPE_INT);

// minute of the hour
DEFINE_UNARY_FN_WITH_IMPL(minuteV2Impl, v) {
    int hour1, mintue1, second1, usec1;
    v.to_time(&hour1, &mintue1, &second1, &usec1);
    return mintue1;
}
DEFINE_TIME_UNARY_FN(minuteV2, TYPE_DATETIME, TYPE_TINYINT);

// second of the minute
DEFINE_UNARY_FN_WITH_IMPL(secondImpl, v) {
    int hour1, mintue1, second1, usec1;
    v.to_time(&hour1, &mintue1, &second1, &usec1);
    return second1;
}
DEFINE_TIME_UNARY_FN(second, TYPE_DATETIME, TYPE_INT);

// second of the minute
DEFINE_UNARY_FN_WITH_IMPL(secondV2Impl, v) {
    int hour1, mintue1, second1, usec1;
    v.to_time(&hour1, &mintue1, &second1, &usec1);
    return second1;
}
DEFINE_TIME_UNARY_FN(secondV2, TYPE_DATETIME, TYPE_TINYINT);

// day_of_week
DEFINE_UNARY_FN_WITH_IMPL(day_of_weekImpl, v) {
    int day = ((DateValue)v).weekday();
    return day + 1;
}
DEFINE_TIME_UNARY_FN(day_of_week, TYPE_DATETIME, TYPE_INT);

// day_of_week_iso
DEFINE_UNARY_FN_WITH_IMPL(day_of_week_isoImpl, v) {
    int day = ((DateValue)v).weekday();
    return (day + 6) % 7 + 1;
}
DEFINE_TIME_UNARY_FN(day_of_week_iso, TYPE_DATETIME, TYPE_INT);

DEFINE_UNARY_FN_WITH_IMPL(time_to_secImpl, v) {
    return static_cast<int64_t>(v);
}
DEFINE_TIME_UNARY_FN(time_to_sec, TYPE_TIME, TYPE_BIGINT);

// month_name
DEFINE_UNARY_FN_WITH_IMPL(month_nameImpl, v) {
    return ((DateValue)v).month_name();
}
DEFINE_TIME_STRING_UNARY_FN(month_name, TYPE_DATETIME, TYPE_VARCHAR);

// day_name
DEFINE_UNARY_FN_WITH_IMPL(day_nameImpl, v) {
    return ((DateValue)v).day_name();
}
DEFINE_TIME_STRING_UNARY_FN(day_name, TYPE_DATETIME, TYPE_VARCHAR);

// day_of_year
DEFINE_UNARY_FN_WITH_IMPL(day_of_yearImpl, v) {
    auto day = (DateValue)v;
    int year, month, day1;
    day.to_date(&year, &month, &day1);
    DateValue first_day_year;
    first_day_year.from_date(year, 1, 1);
    return day.julian() - first_day_year.julian() + 1;
}
DEFINE_TIME_UNARY_FN(day_of_year, TYPE_DATETIME, TYPE_INT);

// week_of_year
DEFINE_UNARY_FN_WITH_IMPL(week_of_yearImpl, v) {
    auto day = (DateValue)v;
    int weeks;
    if (day.get_weeks_of_year_with_cache(&weeks)) {
        return weeks;
    }
    return day.get_week_of_year();
}
DEFINE_TIME_UNARY_FN(week_of_year, TYPE_DATETIME, TYPE_INT);

DEFINE_UNARY_FN_WITH_IMPL(year_week_with_default_modeImpl, t) {
    auto date_value = (DateValue)t;
    int year = 0, month = 0, day = 0;
    date_value.to_date(&year, &month, &day);
    uint to_year = 0;
    int week = TimeFunctions::compute_week(year, month, day, TimeFunctions::week_mode(0 | 2), &to_year);
    return to_year * 100 + week;
}
DEFINE_TIME_UNARY_FN(year_week_with_default_mode, TYPE_DATETIME, TYPE_INT);

DEFINE_BINARY_FUNCTION_WITH_IMPL(year_week_with_modeImpl, t, m) {
    auto date_value = (DateValue)t;
    int year = 0, month = 0, day = 0;
    date_value.to_date(&year, &month, &day);
    uint to_year = 0;
    int week = TimeFunctions::compute_week(year, month, day, TimeFunctions::week_mode(m | 2), &to_year);
    return to_year * 100 + week;
}
DEFINE_TIME_BINARY_FN(year_week_with_mode, TYPE_DATETIME, TYPE_INT, TYPE_INT);

uint TimeFunctions::week_mode(uint mode) {
    uint week_format = (mode & 7);
    if (!(week_format & WEEK_MONDAY_FIRST)) week_format ^= WEEK_FIRST_WEEKDAY;
    return week_format;
}

/*
   Calc days in one year.
   @note Works with both two and four digit years.
   @return number of days in that year
*/
uint TimeFunctions::compute_days_in_year(uint year) {
    return ((year & 3) == 0 && (year % 100 || (year % 400 == 0 && year)) ? TimeFunctions::NUMBER_OF_LEAP_YEAR
                                                                         : TimeFunctions::NUMBER_OF_NON_LEAP_YEAR);
}

/*
   Calc weekday from daynr.
   @retval 0 for Monday
   @retval 6 for Sunday
*/
int TimeFunctions::compute_weekday(long daynr, bool sunday_first_day_of_week) {
    return (static_cast<int>((daynr + 5L + (sunday_first_day_of_week ? 1L : 0L)) % 7));
}

/*
  Calculate nr of day since year 0 in new date-system (from 1615).
  @param year	  Year (exact 4 digit year, no year conversions)
  @param month  Month
  @param day	  Day
  @note 0000-00-00 is a valid date, and will return 0
  @return Days since 0000-00-00
*/
long TimeFunctions::compute_daynr(uint year, uint month, uint day) {
    long delsum;
    int temp;
    int y = year;

    if (y == 0 && month == 0) {
        return 0;
    }

    delsum = static_cast<long>(TimeFunctions::NUMBER_OF_NON_LEAP_YEAR * y + 31 * (static_cast<int>(month) - 1) +
                               static_cast<int>(day));
    if (month <= 2) {
        y--;
    } else {
        delsum -= static_cast<long>(static_cast<int>(month) * 4 + 23) / 10;
    }
    temp = ((y / 100 + 1) * 3) / 4;
    DCHECK(delsum + static_cast<int>(y) / 4 - temp >= 0);
    return (delsum + static_cast<int>(y) / 4 - temp);
}

int32_t TimeFunctions::compute_week(uint year, uint month, uint day, uint week_behaviour, uint* to_year) {
    uint days;
    ulong daynr = TimeFunctions::compute_daynr((uint)year, (uint)month, (uint)day);
    ulong first_daynr = TimeFunctions::compute_daynr((uint)year, 1, 1);
    bool monday_first = (week_behaviour & WEEK_MONDAY_FIRST);
    bool week_year = (week_behaviour & WEEK_YEAR);
    bool first_weekday = (week_behaviour & WEEK_FIRST_WEEKDAY);

    uint weekday = TimeFunctions::compute_weekday(first_daynr, !monday_first);
    uint year_local = year;
    *to_year = year;
    if (month == 1 && day <= 7 - weekday) {
        if (!week_year && ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4))) {
            return 0;
        }
        week_year = true;
        year_local--;
        (*to_year)--;
        first_daynr -= (days = TimeFunctions::compute_days_in_year(year_local));
        weekday = (weekday + 53 * 7 - days) % 7;
    }

    if ((first_weekday && weekday != 0) || (!first_weekday && weekday >= 4)) {
        days = daynr - (first_daynr + (7 - weekday));
    } else {
        days = daynr - (first_daynr - weekday);
    }

    if (week_year && days >= 52 * 7) {
        weekday = (weekday + TimeFunctions::compute_days_in_year(year_local)) % 7;
        if ((!first_weekday && weekday < 4) || (first_weekday && weekday == 0)) {
            year_local++;
            (*to_year)++;
            return 1;
        }
    }
    return days / 7 + 1;
}

DEFINE_UNARY_FN_WITH_IMPL(week_of_year_with_default_modeImpl, t) {
    auto date_value = (DateValue)t;
    int year = 0, month = 0, day = 0;
    date_value.to_date(&year, &month, &day);
    uint to_year = 0;
    return TimeFunctions::compute_week(year, month, day, TimeFunctions::week_mode(0), &to_year);
}
DEFINE_TIME_UNARY_FN(week_of_year_with_default_mode, TYPE_DATETIME, TYPE_INT);

DEFINE_UNARY_FN_WITH_IMPL(week_of_year_isoImpl, t) {
    auto date_value = (DateValue)t;
    int year = 0, month = 0, day = 0;
    date_value.to_date(&year, &month, &day);
    uint to_year = 0;
    return TimeFunctions::compute_week(year, month, day, TimeFunctions::week_mode(3), &to_year);
}

DEFINE_TIME_UNARY_FN(week_of_year_iso, TYPE_DATETIME, TYPE_INT);

DEFINE_BINARY_FUNCTION_WITH_IMPL(week_of_year_with_modeImpl, t, m) {
    auto date_value = (DateValue)t;
    int year = 0, month = 0, day = 0;
    date_value.to_date(&year, &month, &day);
    uint to_year = 0;
    return TimeFunctions::compute_week(year, month, day, TimeFunctions::week_mode(m), &to_year);
}
DEFINE_TIME_BINARY_FN(week_of_year_with_mode, TYPE_DATETIME, TYPE_INT, TYPE_INT);

// to_date
DEFINE_UNARY_FN_WITH_IMPL(to_dateImpl, v) {
    return (DateValue)v;
}
DEFINE_TIME_UNARY_FN(to_date, TYPE_DATETIME, TYPE_DATE);

struct TeradataFormatState {
    std::unique_ptr<TeradataFormat> formatter;
};

// to_tera_date
Status TimeFunctions::to_tera_date_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_notnull_constant_column(1)) {
        return Status::NotSupported("The 2rd argument must be literal");
    }
    auto* state = new TeradataFormatState();
    context->set_function_state(scope, state);
    state->formatter = std::make_unique<TeradataFormat>();
    auto format_col = context->get_constant_column(1);
    auto format_str = ColumnHelper::get_const_value<TYPE_VARCHAR>(format_col);
    if (!state->formatter->prepare(format_str)) {
        return Status::NotSupported(fmt::format("The format parameter {} is invalid", format_str));
    }
    return Status::OK();
}

Status TimeFunctions::to_tera_date_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<TeradataFormatState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

StatusOr<ColumnPtr> TimeFunctions::to_tera_date(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t size = columns[0]->size(); // minimum number of rows.
    ColumnBuilder<TYPE_DATE> result(size);
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto state = reinterpret_cast<TeradataFormatState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (!state->formatter) {
        return Status::InvalidArgument("invalid date format");
    }

    for (size_t i = 0; i < size; ++i) {
        if (str_viewer.is_null(i)) {
            result.append_null();
        } else {
            std::string_view str(str_viewer.value(i));

            DateTimeValue date_time_value;
            if (!state->formatter->parse(str, &date_time_value)) {
                result.append_null();
            } else {
                TimestampValue ts = TimestampValue::create(
                        date_time_value.year(), date_time_value.month(), date_time_value.day(), date_time_value.hour(),
                        date_time_value.minute(), date_time_value.second(), date_time_value.microsecond());
                result.append((DateValue)ts);
            }
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// to_tera_timestamp
Status TimeFunctions::to_tera_timestamp_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_notnull_constant_column(1)) {
        return Status::NotSupported("The 2rd argument must be literal");
    }
    auto* state = new TeradataFormatState();
    context->set_function_state(scope, state);
    state->formatter = std::make_unique<TeradataFormat>();
    auto format_col = context->get_constant_column(1);
    auto format_str = ColumnHelper::get_const_value<TYPE_VARCHAR>(format_col);
    if (!state->formatter->prepare(format_str)) {
        return Status::NotSupported(fmt::format("The format parameter {} is invalid", format_str));
    }
    return Status::OK();
}

// to_tera_timestamp
Status TimeFunctions::to_tera_timestamp_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<TeradataFormatState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

// to_tera_timestamp
StatusOr<ColumnPtr> TimeFunctions::to_tera_timestamp(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t size = columns[0]->size(); // minimum number of rows.
    ColumnBuilder<TYPE_DATETIME> result(size);
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto state = reinterpret_cast<TeradataFormatState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (!state->formatter) {
        return Status::InvalidArgument("invalid datetime format");
    }

    for (size_t i = 0; i < size; ++i) {
        if (str_viewer.is_null(i)) {
            result.append_null();
        } else {
            std::string_view str(str_viewer.value(i));

            DateTimeValue date_time_value;
            if (!state->formatter->parse(str, &date_time_value)) {
                result.append_null();
            } else {
                TimestampValue ts = TimestampValue::create(
                        date_time_value.year(), date_time_value.month(), date_time_value.day(), date_time_value.hour(),
                        date_time_value.minute(), date_time_value.second(), date_time_value.microsecond());
                result.append(ts);
            }
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <TimeUnit UNIT>
TimestampValue timestamp_add(TimestampValue tsv, int count) {
    return tsv.add<UNIT>(count);
}
#define DEFINE_TIME_ADD_FN(FN, UNIT)                                                                               \
    DEFINE_BINARY_FUNCTION_WITH_IMPL(FN##Impl, timestamp, value) { return timestamp_add<UNIT>(timestamp, value); } \
                                                                                                                   \
    DEFINE_TIME_CALC_FN(FN, TYPE_DATETIME, TYPE_INT, TYPE_DATETIME);

#define DEFINE_TIME_SUB_FN(FN, UNIT)                                                                                \
    DEFINE_BINARY_FUNCTION_WITH_IMPL(FN##Impl, timestamp, value) { return timestamp_add<UNIT>(timestamp, -value); } \
                                                                                                                    \
    DEFINE_TIME_CALC_FN(FN, TYPE_DATETIME, TYPE_INT, TYPE_DATETIME);

#define DEFINE_TIME_ADD_AND_SUB_FN(FN_PREFIX, UNIT) \
    DEFINE_TIME_ADD_FN(FN_PREFIX##_add, UNIT);      \
    DEFINE_TIME_SUB_FN(FN_PREFIX##_sub, UNIT);

// years_add
// years_sub
DEFINE_TIME_ADD_AND_SUB_FN(years, TimeUnit::YEAR);

// quarters_add
// quarters_sub
DEFINE_TIME_ADD_AND_SUB_FN(quarters, TimeUnit::QUARTER);

// months_add
// months_sub
DEFINE_TIME_ADD_AND_SUB_FN(months, TimeUnit::MONTH);

// weeks_add
// weeks_sub
DEFINE_TIME_ADD_AND_SUB_FN(weeks, TimeUnit::WEEK);

// days_add
// days_sub
DEFINE_TIME_ADD_AND_SUB_FN(days, TimeUnit::DAY);

// hours_add
// hours_sub
DEFINE_TIME_ADD_AND_SUB_FN(hours, TimeUnit::HOUR);

// minutes_add
// minutes_sub
DEFINE_TIME_ADD_AND_SUB_FN(minutes, TimeUnit::MINUTE);

// seconds_add
// seconds_sub
DEFINE_TIME_ADD_AND_SUB_FN(seconds, TimeUnit::SECOND);

// millis_add
// millis_sub
DEFINE_TIME_ADD_AND_SUB_FN(millis, TimeUnit::MILLISECOND);

// micros_add
// micros_sub
DEFINE_TIME_ADD_AND_SUB_FN(micros, TimeUnit::MICROSECOND);

#undef DEFINE_TIME_ADD_FN
#undef DEFINE_TIME_SUB_FN
#undef DEFINE_TIME_ADD_AND_SUB_FN

Status TimeFunctions::time_slice_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    ColumnPtr column_format = context->get_constant_column(2);
    Slice format_slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column_format);
    auto period_unit = format_slice.to_string();

    std::string time_base;
    if (UNLIKELY(context->get_num_constant_columns() == 3)) {
        time_base = "floor";
    } else {
        ColumnPtr column_time_base = context->get_constant_column(3);
        Slice time_base_slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column_time_base);
        time_base = time_base_slice.to_string();
    }

    ScalarFunction function;
    const FunctionContext::TypeDesc* boundary = context->get_arg_type(0);
    if (boundary->type == LogicalType::TYPE_DATETIME) {
        // floor specify START as the result time.
        if (time_base == "floor") {
            if (period_unit == "microsecond") {
                function = &TimeFunctions::time_slice_datetime_start_microsecond;
            } else if (period_unit == "millisecond") {
                function = &TimeFunctions::time_slice_datetime_start_millisecond;
            } else if (period_unit == "second") {
                function = &TimeFunctions::time_slice_datetime_start_second;
            } else if (period_unit == "minute") {
                function = &TimeFunctions::time_slice_datetime_start_minute;
            } else if (period_unit == "hour") {
                function = &TimeFunctions::time_slice_datetime_start_hour;
            } else if (period_unit == "day") {
                function = &TimeFunctions::time_slice_datetime_start_day;
            } else if (period_unit == "month") {
                function = &TimeFunctions::time_slice_datetime_start_month;
            } else if (period_unit == "year") {
                function = &TimeFunctions::time_slice_datetime_start_year;
            } else if (period_unit == "week") {
                function = &TimeFunctions::time_slice_datetime_start_week;
            } else if (period_unit == "quarter") {
                function = &TimeFunctions::time_slice_datetime_start_quarter;
            } else {
                return Status::InternalError(
                        "period unit must in {microsecond, millisecond, second, minute, hour, day, month, year, week, "
                        "quarter}");
            }
        } else {
            // ceil specify END as the result time.
            DCHECK_EQ(time_base, "ceil");
            if (period_unit == "microsecond") {
                function = &TimeFunctions::time_slice_datetime_end_microsecond;
            } else if (period_unit == "millisecond") {
                function = &TimeFunctions::time_slice_datetime_end_millisecond;
            } else if (period_unit == "second") {
                function = &TimeFunctions::time_slice_datetime_end_second;
            } else if (period_unit == "minute") {
                function = &TimeFunctions::time_slice_datetime_end_minute;
            } else if (period_unit == "hour") {
                function = &TimeFunctions::time_slice_datetime_end_hour;
            } else if (period_unit == "day") {
                function = &TimeFunctions::time_slice_datetime_end_day;
            } else if (period_unit == "month") {
                function = &TimeFunctions::time_slice_datetime_end_month;
            } else if (period_unit == "year") {
                function = &TimeFunctions::time_slice_datetime_end_year;
            } else if (period_unit == "week") {
                function = &TimeFunctions::time_slice_datetime_end_week;
            } else if (period_unit == "quarter") {
                function = &TimeFunctions::time_slice_datetime_end_quarter;
            } else {
                return Status::InternalError(
                        "period unit must in {microsecond, millisecond, second, minute, hour, day, month, year, week, "
                        "quarter}");
            }
        }
    } else {
        DCHECK_EQ(boundary->type, LogicalType::TYPE_DATE);
        if (time_base == "floor") {
            if (period_unit == "second" || period_unit == "minute" || period_unit == "hour") {
                return Status::InvalidArgument("can't use time_slice for date with time(hour/minute/second)");
            } else if (period_unit == "day") {
                function = &TimeFunctions::time_slice_date_start_day;
            } else if (period_unit == "month") {
                function = &TimeFunctions::time_slice_date_start_month;
            } else if (period_unit == "year") {
                function = &TimeFunctions::time_slice_date_start_year;
            } else if (period_unit == "week") {
                function = &TimeFunctions::time_slice_date_start_week;
            } else if (period_unit == "quarter") {
                function = &TimeFunctions::time_slice_date_start_quarter;
            } else {
                return Status::InternalError(
                        "period unit must in {second, minute, hour, day, month, year, week, quarter}");
            }
        } else {
            DCHECK_EQ(time_base, "ceil");
            if (period_unit == "second" || period_unit == "minute" || period_unit == "hour") {
                return Status::InvalidArgument("can't use time_slice for date with time(hour/minute/second)");
            } else if (period_unit == "day") {
                function = &TimeFunctions::time_slice_date_end_day;
            } else if (period_unit == "month") {
                function = &TimeFunctions::time_slice_date_end_month;
            } else if (period_unit == "year") {
                function = &TimeFunctions::time_slice_date_end_year;
            } else if (period_unit == "week") {
                function = &TimeFunctions::time_slice_date_end_week;
            } else if (period_unit == "quarter") {
                function = &TimeFunctions::time_slice_date_end_quarter;
            } else {
                return Status::InternalError(
                        "period unit must in {second, minute, hour, day, month, year, week, quarter}");
            }
        }
    }

    auto fc = new DateTruncCtx();
    fc->function = function;
    context->set_function_state(scope, fc);
    return Status::OK();
}

#define DEFINE_TIME_SLICE_FN_CALL(TypeName, UNIT, LType, RType, ResultType)                                      \
    StatusOr<ColumnPtr> TimeFunctions::time_slice_##TypeName##_start_##UNIT(FunctionContext* context,            \
                                                                            const starrocks::Columns& columns) { \
        return time_slice_function_##UNIT<LType, RType, ResultType, true>(context, columns);                     \
    }                                                                                                            \
    StatusOr<ColumnPtr> TimeFunctions::time_slice_##TypeName##_end_##UNIT(FunctionContext* context,              \
                                                                          const starrocks::Columns& columns) {   \
        return time_slice_function_##UNIT<LType, RType, ResultType, false>(context, columns);                    \
    }

#define DEFINE_TIME_SLICE_FN(UNIT)                                                                     \
    template <LogicalType LType, LogicalType RType, LogicalType ResultType, bool is_start>             \
    StatusOr<ColumnPtr> time_slice_function_##UNIT(FunctionContext* context, const Columns& columns) { \
        auto time_viewer = ColumnViewer<LType>(columns[0]);                                            \
        auto period_viewer = ColumnViewer<RType>(columns[1]);                                          \
        auto size = columns[0]->size();                                                                \
        ColumnBuilder<ResultType> results(size);                                                       \
        for (int row = 0; row < size; row++) {                                                         \
            if (time_viewer.is_null(row) || period_viewer.is_null(row)) {                              \
                results.append_null();                                                                 \
                continue;                                                                              \
            }                                                                                          \
            TimestampValue time_value = time_viewer.value(row);                                        \
            auto period_value = period_viewer.value(row);                                              \
            if (time_value.diff_microsecond(TimeFunctions::start_of_time_slice) < 0) {                 \
                return Status::InvalidArgument(TimeFunctions::info_reported_by_time_slice);            \
            }                                                                                          \
            time_value.template floor_to_##UNIT##_period<!is_start>(period_value);                     \
            results.append(time_value);                                                                \
        }                                                                                              \
        return date_valid<ResultType>(results.build(ColumnHelper::is_all_const(columns)));             \
    }                                                                                                  \
    DEFINE_TIME_SLICE_FN_CALL(datetime, UNIT, TYPE_DATETIME, TYPE_INT, TYPE_DATETIME);                 \
    DEFINE_TIME_SLICE_FN_CALL(date, UNIT, TYPE_DATE, TYPE_INT, TYPE_DATE);

// time_slice_to_second
DEFINE_TIME_SLICE_FN(microsecond);

// time_slice_to_second
DEFINE_TIME_SLICE_FN(millisecond);

// time_slice_to_second
DEFINE_TIME_SLICE_FN(second);

// time_slice_to_minute
DEFINE_TIME_SLICE_FN(minute);

// time_slice_to_hour
DEFINE_TIME_SLICE_FN(hour);

// time_slice_to_day
DEFINE_TIME_SLICE_FN(day);

// time_slice_to_week
DEFINE_TIME_SLICE_FN(week);

// time_slice_to_month
DEFINE_TIME_SLICE_FN(month);

// time_slice_to_quarter
DEFINE_TIME_SLICE_FN(quarter);

// time_slice_to_year
DEFINE_TIME_SLICE_FN(year);

#undef DEFINE_TIME_SLICE_FN

StatusOr<ColumnPtr> TimeFunctions::time_slice(FunctionContext* context, const Columns& columns) {
    auto ctc = reinterpret_cast<DateTruncCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    return ctc->function(context, columns);
}

Status TimeFunctions::time_slice_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto fc = reinterpret_cast<DateTruncCtx*>(context->get_function_state(scope));
        delete fc;
    }

    return Status::OK();
}

// years_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(years_diffImpl, l, r) {
    int year1, month1, day1, hour1, minute1, second1, usec1;
    int year2, month2, day2, hour2, minute2, second2, usec2;
    l.to_timestamp(&year1, &month1, &day1, &hour1, &minute1, &second1, &usec1);
    r.to_timestamp(&year2, &month2, &day2, &hour2, &minute2, &second2, &usec2);

    int year = (year1 - year2);

    const auto func = [](int month, int day, int hour, int minute, int second, int usec) -> int64_t {
        return int64_t(month) * 100'000'000'000'000LL + day * 1'000'000'000'000LL + hour * 10'000'000'000LL +
               minute * 100'000'000LL + second * 1'000'000LL + usec;
    };

    if (year >= 0) {
        year -= (func(month1, day1, hour1, minute1, second1, usec1) <
                 func(month2, day2, hour2, minute2, second2, usec2));
    } else {
        year += (func(month1, day1, hour1, minute1, second1, usec1) >
                 func(month2, day2, hour2, minute2, second2, usec2));
    }
    return year;
}

DEFINE_TIME_BINARY_FN(years_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// years_diff_v2
DEFINE_BINARY_FUNCTION_WITH_IMPL(years_diff_v2Impl, to_timestamp, from_timestamp) {
    int8_t sign = from_timestamp < to_timestamp ? 1 : -1;

    int year1, month1, day1, hour1, minute1, second1, usec1;
    std::min(from_timestamp, to_timestamp).to_timestamp(&year1, &month1, &day1, &hour1, &minute1, &second1, &usec1);
    int64_t us_of_day1 = 1LL * hour1 * 3600000 + minute1 * 60000 + second1 * 1000 + usec1;
    int32_t last_day_of_month1 = DAYS_IN_MONTH[date::is_leap(year1)][month1];

    int year2, month2, day2, hour2, minute2, second2, usec2;
    std::max(from_timestamp, to_timestamp).to_timestamp(&year2, &month2, &day2, &hour2, &minute2, &second2, &usec2);
    int64_t us_of_day2 = 1LL * hour2 * 3600000 + minute2 * 60000 + second2 * 1000 + usec2;
    int32_t last_day_of_month2 = DAYS_IN_MONTH[date::is_leap(year2)][month2];

    int64_t diff = year2 - year1;

    // too many special cases, need handle it carefully
    // @TODO(silverbullet233): how to simplify so many if-else
    if (month1 > month2) {
        diff--;
    } else if (month1 == month2) {
        if (last_day_of_month1 != last_day_of_month2) {
            DCHECK_EQ(month1, 2);
            // The number of days in Feb is different in normal years and leap years,
            // and requires special handling
            if (day1 > day2) {
                if (day2 != last_day_of_month2) {
                    diff--;
                } else {
                    if (day1 == last_day_of_month1 && us_of_day1 > us_of_day2) {
                        // date_diff('year', '2017-02-28', '2016-02-29 00:00:00.01') should return 0
                        diff--;
                    }
                    // date_diff('year', '2017-02-28', '2016-02-29') should return 1
                }
            } else if (day1 == day2) {
                if (day2 != last_day_of_month2) {
                    if (us_of_day1 > us_of_day2) {
                        // date_diff('year', '2016-02-28', '2015-02-28 00:00:00.01') should return 0
                        diff--;
                    }
                    // date_diff('year', '2016-02-28', '2015-02-28') should return 1
                }
            }
        } else {
            if ((day1 > day2) || (day1 == day2 && us_of_day1 > us_of_day2)) {
                diff--;
            }
        }
    }

    return diff * sign;
}

DEFINE_TIME_BINARY_FN(years_diff_v2, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// months_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(months_diffImpl, l, r) {
    int year1, month1, day1, hour1, minute1, second1, usec1;
    int year2, month2, day2, hour2, minute2, second2, usec2;
    l.to_timestamp(&year1, &month1, &day1, &hour1, &minute1, &second1, &usec1);
    r.to_timestamp(&year2, &month2, &day2, &hour2, &minute2, &second2, &usec2);

    int month = (year1 - year2) * 12 + (month1 - month2);
    const auto func = [](int day, int hour, int minute, int second, int usec) -> int64_t {
        return int64_t(day) * 1'000'000'000'000LL + hour * 10'000'000'000LL + minute * 100'000'000LL +
               second * 1'000'000LL + usec;
    };

    if (month >= 0) {
        month -= (func(day1, hour1, minute1, second1, usec1) < func(day2, hour2, minute2, second2, usec2));
    } else {
        month += (func(day1, hour1, minute1, second1, usec1) > func(day2, hour2, minute2, second2, usec2));
    }

    return month;
}

DEFINE_TIME_BINARY_FN(months_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// months_diff_v2
DEFINE_BINARY_FUNCTION_WITH_IMPL(months_diff_v2Impl, to_timestamp, from_timestamp) {
    int8_t sign = from_timestamp < to_timestamp ? 1 : -1;

    int year1, month1, day1, hour1, mintue1, second1, usec1;
    std::min(from_timestamp, to_timestamp).to_timestamp(&year1, &month1, &day1, &hour1, &mintue1, &second1, &usec1);
    int64_t us_of_day1 = 1LL * hour1 * 3600000 + mintue1 * 60000 + second1 * 1000 + usec1;
    int32_t last_day_of_month1 = DAYS_IN_MONTH[date::is_leap(year1)][month1];

    int year2, month2, day2, hour2, mintue2, second2, usec2;
    std::max(from_timestamp, to_timestamp).to_timestamp(&year2, &month2, &day2, &hour2, &mintue2, &second2, &usec2);
    int64_t us_of_day2 = 1LL * hour2 * 3600000 + mintue2 * 60000 + second2 * 1000 + usec2;

    int32_t last_day_of_month2 = DAYS_IN_MONTH[date::is_leap(year2)][month2];

    int64_t diff = (year2 - year1) * 12 + (month2 - month1);

    // too many special cases, need handle it carefully
    // @TODO(silverbullet233): how to simplify so many if-else
    if (day1 > day2) {
        if (day2 != last_day_of_month2) {
            // the most common cases
            diff--;
        } else {
            if (day1 == last_day_of_month1 && us_of_day1 > us_of_day2) {
                // both are the last day of their respective month
                // date_diff('month', '2017-02-28', '2016-02-29 00:00:00.01') should return 11
                diff--;
            }
            // other cases
            // date_diff('month', '2017-02-28', '2016-02-29') should return 12
            // date_diff('month', '2017-02-28', '2016-02-28 00:00:00.01') should return 12
        }
    } else if (day1 == day2) {
        if (day2 == last_day_of_month2) {
            if (day1 == last_day_of_month1) {
                // both are the last day of their respective month
                if (us_of_day1 > us_of_day2) {
                    // date_diff('month', '2016-06-30', '2016-04-30 00:00:00.01') should return 1
                    diff--;
                }
                // date_diff('month', '2016-06-30', '2016-04-30') should return 2
            }
            // date_diff('month', '2017-02-28', '2016-02-28') should return 12
            // date_diff('month', '2016-06-30', '2016-05-30 00:00:00.01') should return 1
            // date_diff('month', '2016-02-29', '2016-01-29 00:00:00.01') should return 1
        } else {
            if (us_of_day1 > us_of_day2) {
                // date_diff('month', '2016-02-28', '2016-01-28 00:00:00.01') should return 0
                diff--;
            }
            // date_diff('month', '2016-02-28', '2016-01-28') should return 1
        }
    }

    return sign * diff;
}

DEFINE_TIME_BINARY_FN(months_diff_v2, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// quarters_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(quarters_diffImpl, l, r) {
    auto diff = months_diffImpl::apply<LType, RType, ResultType>(l, r);
    return diff / 3;
}
DEFINE_TIME_BINARY_FN(quarters_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

DEFINE_BINARY_FUNCTION_WITH_IMPL(quarters_diff_v2Impl, l, r) {
    return months_diff_v2Impl::apply<LType, RType, ResultType>(l, r) / 3;
}
DEFINE_TIME_BINARY_FN(quarters_diff_v2, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// weeks_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(weeks_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_WEEK;
}
DEFINE_TIME_BINARY_FN(weeks_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// days_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(days_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_DAY;
}
DEFINE_TIME_BINARY_FN(days_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// date_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(date_diffImpl, l, r) {
    return ((DateValue)l).julian() - ((DateValue)r).julian();
}
DEFINE_TIME_BINARY_FN(date_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_INT);

// time_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(time_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_SEC;
}
DEFINE_TIME_BINARY_FN(time_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_TIME);

// hours_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(hours_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_HOUR;
}
DEFINE_TIME_BINARY_FN(hours_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// minutes_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(minutes_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_MINUTE;
}
DEFINE_TIME_BINARY_FN(minutes_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// seconds_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(seconds_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_SEC;
}
DEFINE_TIME_BINARY_FN(seconds_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

// milliseconds_diff
DEFINE_BINARY_FUNCTION_WITH_IMPL(milliseconds_diffImpl, l, r) {
    return l.diff_microsecond(r) / USECS_PER_MILLIS;
}
DEFINE_TIME_BINARY_FN(milliseconds_diff, TYPE_DATETIME, TYPE_DATETIME, TYPE_BIGINT);

/*
 * definition for to_unix operators(SQL TYPE: DATETIME)
 */
template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_to_unix_from_datetime(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);

    auto date_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TIMESTAMP_TYPE> result(size);
    for (int row = 0; row < size; ++row) {
        if (date_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = date_viewer.value(row);

        int year, month, day, hour, minute, second, usec;
        date.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
        DateTimeValue tv(TIME_DATETIME, year, month, day, hour, minute, second, usec);

        int64_t timestamp;
        if (!tv.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
            result.append_null();
        } else {
            timestamp = timestamp < 0 ? 0 : timestamp;
            timestamp = timestamp > MAX_UNIX_TIMESTAMP ? 0 : timestamp;
            result.append(timestamp);
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_from_datetime_64(FunctionContext* context, const Columns& columns) {
    return _t_to_unix_from_datetime<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_from_datetime_32(FunctionContext* context, const Columns& columns) {
    return _t_to_unix_from_datetime<TYPE_INT>(context, columns);
}

/*
 * definition for to_unix operators(SQL TYPE: DATE)
 */
template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_to_unix_from_date(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);

    auto date_viewer = ColumnViewer<TYPE_DATE>(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TIMESTAMP_TYPE> result(size);
    for (int row = 0; row < size; ++row) {
        if (date_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = date_viewer.value(row);

        int year, month, day;
        date.to_date(&year, &month, &day);
        DateTimeValue tv(TIME_DATE, year, month, day, 0, 0, 0, 0);

        int64_t timestamp;
        if (!tv.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
            result.append_null();
        } else {
            timestamp = timestamp < 0 ? 0 : timestamp;
            timestamp = timestamp > MAX_UNIX_TIMESTAMP ? 0 : timestamp;
            result.append(timestamp);
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_from_date_64(FunctionContext* context, const Columns& columns) {
    return _t_to_unix_from_date<TYPE_BIGINT>(context, columns);
}
StatusOr<ColumnPtr> TimeFunctions::to_unix_from_date_32(FunctionContext* context, const Columns& columns) {
    return _t_to_unix_from_date<TYPE_INT>(context, columns);
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_to_unix_from_datetime_with_format(FunctionContext* context,
                                                                        const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto date_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto formatViewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TIMESTAMP_TYPE> result(size);
    for (int row = 0; row < size; ++row) {
        if (date_viewer.is_null(row) || formatViewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = date_viewer.value(row);
        auto format = formatViewer.value(row);
        if (date.empty() || format.empty()) {
            result.append_null();
            continue;
        }
        DateTimeValue tv;
        if (!tv.from_date_format_str(format.data, format.size, date.data, date.size)) {
            result.append_null();
            continue;
        }
        int64_t timestamp;
        if (!tv.unix_timestamp(&timestamp, context->state()->timezone_obj())) {
            result.append_null();
            continue;
        }

        timestamp = timestamp < 0 ? 0 : timestamp;
        timestamp = timestamp > MAX_UNIX_TIMESTAMP ? 0 : timestamp;
        result.append(timestamp);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_from_datetime_with_format_64(FunctionContext* context,
                                                                        const Columns& columns) {
    return _t_to_unix_from_datetime_with_format<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_from_datetime_with_format_32(FunctionContext* context,
                                                                        const Columns& columns) {
    return _t_to_unix_from_datetime_with_format<TYPE_INT>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_for_now_64(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 0);
    int64_t value = context->state()->timestamp_ms() / 1000;
    auto result = Int64Column::create();
    result->append(value);
    return ConstColumn::create(std::move(result), 1);
}

StatusOr<ColumnPtr> TimeFunctions::to_unix_for_now_32(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 0);
    int64_t value = context->state()->timestamp_ms() / 1000;
    auto result = Int32Column::create();
    result->append(value);
    return ConstColumn::create(std::move(result), 1);
}

/*
 * end definition for to_unix operators
 */

/*
 * definition for from_unix operators
 */
template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_to_datetime(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);

    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    ColumnViewer<TIMESTAMP_TYPE> data_column(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row);
        if (date < 0 || date > MAX_UNIX_TIMESTAMP) {
            result.append_null();
            continue;
        }

        DateTimeValue dtv;
        if (!dtv.from_unixtime(date, context->state()->timezone_obj())) {
            result.append_null();
            continue;
        }
        char buf[64];
        dtv.to_string(buf);
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_to_datetime_ms(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TIMESTAMP_TYPE> data_column(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row) / 1000;
        if (date < 0 || date > MAX_UNIX_TIMESTAMP) {
            result.append_null();
            continue;
        }

        DateTimeValue dtv;
        if (!dtv.from_unixtime(date, context->state()->timezone_obj())) {
            result.append_null();
            continue;
        }
        char buf[64];
        dtv.to_string(buf);
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> TimeFunctions::from_unix_to_datetime_64(FunctionContext* context, const Columns& columns) {
    return _t_from_unix_to_datetime<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::from_unix_to_datetime_32(FunctionContext* context, const Columns& columns) {
    return _t_from_unix_to_datetime<TYPE_INT>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::from_unix_to_datetime_ms_64(FunctionContext* context, const Columns& columns) {
    return _t_from_unix_to_datetime_ms<TYPE_BIGINT>(context, columns);
}

static inline int64_t impl_hour_from_unixtime(int64_t unixtime) {
    // return (unixtime % 86400) / 3600;
    static const libdivide::divider<int64_t> fast_div_3600(3600);
    static const libdivide::divider<int64_t> fast_div_86400(86400);

    // Handle negative unixtime correctly by ensuring positive remainder
    int64_t remainder;
    if (LIKELY(unixtime >= 0)) {
        remainder = unixtime - unixtime / fast_div_86400 * 86400;
    } else {
        remainder = unixtime % 86400;
        if (remainder < 0) {
            remainder += 86400;
        }
    }
    int64_t hour = remainder / fast_div_3600;
    return hour;
}

StatusOr<ColumnPtr> TimeFunctions::hour_from_unixtime(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    static const auto epoch =
            std::chrono::time_point_cast<cctz::sys_seconds>(std::chrono::system_clock::from_time_t(0));

    auto ctz = context->state()->timezone_obj();
    auto size = columns[0]->size();
    ColumnViewer<TYPE_BIGINT> data_column(columns[0]);
    ColumnBuilder<TYPE_INT> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row);
        if (date < 0 || date > MAX_UNIX_TIMESTAMP) {
            result.append_null();
            continue;
        }

        cctz::time_point<cctz::sys_seconds> t = epoch + cctz::seconds(date);
        int offset = ctz.lookup_offset(t).offset;
        int hour = impl_hour_from_unixtime(date + offset);
        result.append(hour);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

std::string TimeFunctions::convert_format(const Slice& format) {
    switch (format.get_size()) {
    case 8:
        if (strncmp((const char*)format.get_data(), "yyyyMMdd", 8) == 0) {
            std::string tmp("%Y%m%d");
            return tmp;
        }
        break;
    case 10:
        if (strncmp((const char*)format.get_data(), "yyyy-MM-dd", 10) == 0) {
            std::string tmp("%Y-%m-%d");
            return tmp;
        }
        break;
    case 19:
        if (strncmp((const char*)format.get_data(), "yyyy-MM-dd HH:mm:ss", 19) == 0) {
            std::string tmp("%Y-%m-%d %H:%i:%s");
            return tmp;
        }
        break;
    default:
        break;
    }
    return format.to_string();
}

// regex method
Status TimeFunctions::from_unix_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new FromUnixState();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    state->const_format = true;
    auto column = context->get_constant_column(1);
    auto format = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);

    if (format.size > DEFAULT_DATE_FORMAT_LIMIT) {
        return Status::InvalidArgument("Time format invalid");
    }

    state->format_content = convert_format(format);
    return Status::OK();
}

Status TimeFunctions::from_unix_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<FromUnixState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

Status TimeFunctions::from_unix_timezone_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new FromUnixState();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1) || !context->is_notnull_constant_column(2)) {
        return Status::OK();
    }

    auto column = context->get_constant_column(1);
    auto format = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);

    if (format.size > DEFAULT_DATE_FORMAT_LIMIT) {
        return Status::InvalidArgument("Time format invalid");
    }

    state->format_content = convert_format(format);

    auto timezone_column = context->get_constant_column(2);
    auto timezone = ColumnHelper::get_const_value<TYPE_VARCHAR>(timezone_column);

    state->timezone_content = timezone.to_string();

    state->const_format = true;
    state->const_timezone = true;

    return Status::OK();
}

Status TimeFunctions::from_unix_timezone_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* state = reinterpret_cast<FromUnixState*>(context->get_function_state(scope));
        delete state;
    }
    return Status::OK();
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_with_format_general(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TIMESTAMP_TYPE> data_column(columns[0]);
    ColumnViewer<TYPE_VARCHAR> format_column(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row) || format_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row);
        auto format = format_column.value(row);
        if (date < 0 || date > MAX_UNIX_TIMESTAMP || format.empty()) {
            result.append_null();
            continue;
        }

        DateTimeValue dtv;
        if (!dtv.from_unixtime(date, context->state()->timezone_obj())) {
            result.append_null();
            continue;
        }
        // use lambda to avoid adding method for TimeFunctions.
        if (format.size > DEFAULT_DATE_FORMAT_LIMIT) {
            result.append_null();
            continue;
        }

        std::string new_fmt = convert_format(format);

        char buf[128];
        if (!dtv.to_format_string((const char*)new_fmt.c_str(), new_fmt.size(), buf)) {
            result.append_null();
            continue;
        }
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_with_format_const(std::string& format_content, FunctionContext* context,
                                                                  const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TIMESTAMP_TYPE> data_column(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row) || format_content.empty()) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row);
        if (date < 0 || date > MAX_UNIX_TIMESTAMP) {
            result.append_null();
            continue;
        }

        DateTimeValue dtv;
        if (!dtv.from_unixtime(date, context->state()->timezone_obj())) {
            result.append_null();
            continue;
        }

        char buf[128];
        if (!dtv.to_format_string((const char*)format_content.c_str(), format_content.size(), buf)) {
            result.append_null();
            continue;
        }
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_with_format_timezone(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 3);

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TIMESTAMP_TYPE> data_column(columns[0]);
    ColumnViewer<TYPE_VARCHAR> format_column(columns[1]);
    ColumnViewer<TYPE_VARCHAR> timezone_column(columns[2]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row) || format_column.is_null(row) || timezone_column.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row);
        auto format = format_column.value(row);
        if (date < 0 || date > MAX_UNIX_TIMESTAMP || format.empty()) {
            result.append_null();
            continue;
        }

        std::string timezone = timezone_column.value(row).to_string();
        cctz::time_zone timezone_obj;
        TimezoneUtils::find_cctz_time_zone(timezone, timezone_obj);

        DateTimeValue dtv;
        if (!dtv.from_unixtime(date, timezone_obj)) {
            result.append_null();
            continue;
        }
        // use lambda to avoid adding method for TimeFunctions.
        if (format.size > DEFAULT_DATE_FORMAT_LIMIT) {
            result.append_null();
            continue;
        }

        std::string new_fmt = convert_format(format);

        char buf[128];
        if (!dtv.to_format_string((const char*)new_fmt.c_str(), new_fmt.size(), buf)) {
            result.append_null();
            continue;
        }
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_with_format_timezone_const(const std::string& format_content,
                                                                           const std::string& timezone_content,
                                                                           FunctionContext* context,
                                                                           const Columns& columns) {
    DCHECK_EQ(columns.size(), 3);

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    ColumnViewer<TIMESTAMP_TYPE> data_column(columns[0]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row) || format_content.empty() || timezone_content.empty()) {
            result.append_null();
            continue;
        }

        auto date = data_column.value(row);
        if (date < 0 || date > MAX_UNIX_TIMESTAMP) {
            result.append_null();
            continue;
        }

        cctz::time_zone timezone_obj;
        TimezoneUtils::find_cctz_time_zone(timezone_content, timezone_obj);

        DateTimeValue dtv;
        if (!dtv.from_unixtime(date, timezone_obj)) {
            result.append_null();
            continue;
        }

        char buf[128];
        if (!dtv.to_format_string((const char*)format_content.c_str(), format_content.size(), buf)) {
            result.append_null();
            continue;
        }
        result.append(Slice(buf));
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_t_from_unix_with_format(FunctionContext* context,
                                                            const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    auto* state = reinterpret_cast<FromUnixState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state->const_format) {
        std::string format_content = state->format_content;
        return _t_from_unix_with_format_const<TIMESTAMP_TYPE>(format_content, context, columns);
    }
    return _t_from_unix_with_format_general<TIMESTAMP_TYPE>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::from_unix_to_datetime_with_format_timezone(FunctionContext* context,
                                                                              const starrocks::Columns& columns) {
    DCHECK_EQ(columns.size(), 3);
    auto* state = reinterpret_cast<FromUnixState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state->const_format && state->const_timezone) {
        return _t_from_unix_with_format_timezone_const<TYPE_BIGINT>(state->format_content, state->timezone_content,
                                                                    context, columns);
    }
    return _t_from_unix_with_format_timezone<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::from_unix_to_datetime_with_format_64(FunctionContext* context,
                                                                        const starrocks::Columns& columns) {
    return _t_from_unix_with_format<TYPE_BIGINT>(context, columns);
}
StatusOr<ColumnPtr> TimeFunctions::from_unix_to_datetime_with_format_32(FunctionContext* context,
                                                                        const starrocks::Columns& columns) {
    return _t_from_unix_with_format<TYPE_INT>(context, columns);
}

/*
 * end definition for from_unix operators
 */
constexpr int64_t MICROSECONDS_PER_SECOND = 1000000LL;
constexpr int64_t MICROSECONDS_PER_MILLISECOND = 1000LL;

constexpr bool is_valid_scale(int scale) {
    return scale == 0 || scale == 3 || scale == 6;
}

constexpr int64_t get_scale_factor(int scale) {
    switch (scale) {
    case 0:
        return 1;
    case 3:
        return 1000;
    case 6:
        return 1000000;
    default:
        return -1;
    }
}

inline std::pair<int64_t, int64_t> safe_divmod(int64_t dividend, int64_t divisor) {
    int64_t quotient = dividend / divisor;
    int64_t remainder = dividend % divisor;
    if (remainder < 0) {
        quotient -= 1;
        remainder += divisor;
    }
    return {quotient, remainder};
}

inline void normalize_microseconds(int64_t& seconds, int64_t& microseconds) {
    if (microseconds >= MICROSECONDS_PER_SECOND) {
        int64_t overflow_seconds = microseconds / MICROSECONDS_PER_SECOND;
        seconds += overflow_seconds;
        microseconds %= MICROSECONDS_PER_SECOND;
    } else if (microseconds < 0) {
        int64_t borrow_seconds = (-microseconds + MICROSECONDS_PER_SECOND - 1) / MICROSECONDS_PER_SECOND;
        seconds -= borrow_seconds;
        microseconds += borrow_seconds * MICROSECONDS_PER_SECOND;
    }
}

struct UnixTimeConversionContext {
    bool has_scale_column = false;
    bool scale_is_const = false;
    int const_scale = 0;
    bool result_is_null = false;
    bool timezone_aware = false;
    cctz::time_zone session_timezone;

    using ConversionFuncPtr = std::pair<int64_t, int64_t> (*)(int64_t);
    static ConversionFuncPtr scale_conversion_funcs[3];
    int scale_index = 0;

    ConversionFuncPtr active_conversion_func = nullptr;
};

inline std::pair<int64_t, int64_t> convert_timestamp_scale_0(int64_t timestamp_value) {
    return {timestamp_value, 0};
}

inline std::pair<int64_t, int64_t> convert_timestamp_scale_3(int64_t timestamp_value) {
    auto [seconds, remainder] = safe_divmod(timestamp_value, MICROSECONDS_PER_MILLISECOND);
    int64_t microseconds = remainder * MICROSECONDS_PER_MILLISECOND;
    normalize_microseconds(seconds, microseconds);
    return {seconds, microseconds};
}

inline std::pair<int64_t, int64_t> convert_timestamp_scale_6(int64_t timestamp_value) {
    auto [seconds, remainder] = safe_divmod(timestamp_value, MICROSECONDS_PER_SECOND);
    normalize_microseconds(seconds, remainder);
    return {seconds, remainder};
}

UnixTimeConversionContext::ConversionFuncPtr UnixTimeConversionContext::scale_conversion_funcs[3] = {
        convert_timestamp_scale_0, convert_timestamp_scale_3, convert_timestamp_scale_6};

Status TimeFunctions::_unixtime_to_datetime_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope,
                                                    bool timezone_aware) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* conv_ctx = new UnixTimeConversionContext();
    conv_ctx->timezone_aware = timezone_aware;
    if (timezone_aware) {
        conv_ctx->session_timezone = context->state()->timezone_obj();
    }

    context->set_function_state(scope, conv_ctx);

    int num_args = context->get_num_args();
    if (num_args < 1 || num_args > 2) {
        return Status::InvalidArgument("unixtime_to_datetime expects 1 or 2 arguments");
    }

    conv_ctx->has_scale_column = context->get_num_args() == 2;

    if (conv_ctx->has_scale_column) {
        if (context->is_constant_column(1)) {
            conv_ctx->scale_is_const = true;

            if (!context->is_notnull_constant_column(1)) {
                conv_ctx->result_is_null = true;
                return Status::OK();
            }

            conv_ctx->const_scale = ColumnHelper::get_const_value<TYPE_INT>(context->get_constant_column(1));

            if (!is_valid_scale(conv_ctx->const_scale)) {
                conv_ctx->result_is_null = true;
                return Status::OK();
            }

            int scale_index = -1;
            switch (conv_ctx->const_scale) {
            case 0:
                scale_index = 0;
                break;
            case 3:
                scale_index = 1;
                break;
            case 6:
                scale_index = 2;
                break;
            default:
                conv_ctx->result_is_null = true;
                return Status::OK();
            }
            conv_ctx->scale_index = scale_index;
        }
    } else {
        conv_ctx->scale_is_const = true;
        conv_ctx->const_scale = 0;
        conv_ctx->scale_index = 0;
    }

    if (conv_ctx->scale_is_const) {
        conv_ctx->active_conversion_func = UnixTimeConversionContext::scale_conversion_funcs[conv_ctx->scale_index];
    }

    return Status::OK();
}

Status TimeFunctions::_unixtime_to_datetime_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* conv_ctx = reinterpret_cast<UnixTimeConversionContext*>(context->get_function_state(scope));
        if (conv_ctx) {
            delete conv_ctx;
            context->set_function_state(scope, nullptr);
        }
    }
    return Status::OK();
}

template <LogicalType TIMESTAMP_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_unixtime_to_datetime(FunctionContext* context, const Columns& columns) {
    DCHECK_GE(columns.size(), 1);
    DCHECK_LE(columns.size(), 2);

    auto* conv_ctx =
            reinterpret_cast<UnixTimeConversionContext*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (!conv_ctx) {
        return Status::InternalError("Function context not properly initialized");
    }

    auto size = columns[0]->size();
    if (conv_ctx->result_is_null) {
        return ColumnHelper::create_const_null_column(size);
    }

    ColumnViewer<TIMESTAMP_TYPE> timestamp_viewer(columns[0]);
    ColumnBuilder<TYPE_DATETIME> result(size);

    if (conv_ctx->scale_is_const) {
        auto conversion_func = conv_ctx->active_conversion_func;

        for (int row = 0; row < size; ++row) {
            if (timestamp_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            auto [seconds, microseconds] = conversion_func(timestamp_viewer.value(row));

            TimestampValue result_timestamp;
            if (conv_ctx->timezone_aware) {
                result_timestamp.from_unixtime(seconds, microseconds, conv_ctx->session_timezone);
            } else {
                result_timestamp.from_unix_second(seconds, microseconds);
            }

            if (result_timestamp.is_valid()) {
                result.append(result_timestamp);
            } else {
                result.append_null();
            }
        }
    } else {
        ColumnViewer<TYPE_INT> scale_viewer(columns[1]);

        for (int row = 0; row < size; ++row) {
            if (timestamp_viewer.is_null(row) || scale_viewer.is_null(row)) {
                result.append_null();
                continue;
            }

            int current_scale = scale_viewer.value(row);

            int scale_idx = -1;
            switch (current_scale) {
            case 0:
                scale_idx = 0;
                break;
            case 3:
                scale_idx = 1;
                break;
            case 6:
                scale_idx = 2;
                break;
            default:
                result.append_null();
                continue;
            }

            auto [seconds, microseconds] =
                    UnixTimeConversionContext::scale_conversion_funcs[scale_idx](timestamp_viewer.value(row));

            TimestampValue result_timestamp;
            if (conv_ctx->timezone_aware) {
                result_timestamp.from_unixtime(seconds, microseconds, conv_ctx->session_timezone);
            } else {
                result_timestamp.from_unix_second(seconds, microseconds);
            }

            if (result_timestamp.is_valid()) {
                result.append(result_timestamp);
            } else {
                result.append_null();
            }
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

Status TimeFunctions::unixtime_to_datetime_prepare(FunctionContext* context,
                                                   FunctionContext::FunctionStateScope scope) {
    return _unixtime_to_datetime_prepare(context, scope, true);
}
Status TimeFunctions::unixtime_to_datetime_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    return _unixtime_to_datetime_close(context, scope);
}
StatusOr<ColumnPtr> TimeFunctions::unixtime_to_datetime(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    return _unixtime_to_datetime<TYPE_BIGINT>(context, columns);
}

Status TimeFunctions::unixtime_to_datetime_ntz_prepare(FunctionContext* context,
                                                       FunctionContext::FunctionStateScope scope) {
    return _unixtime_to_datetime_prepare(context, scope, false);
}
Status TimeFunctions::unixtime_to_datetime_ntz_close(FunctionContext* context,
                                                     FunctionContext::FunctionStateScope scope) {
    return _unixtime_to_datetime_close(context, scope);
}

StatusOr<ColumnPtr> TimeFunctions::unixtime_to_datetime_ntz(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    return _unixtime_to_datetime<TYPE_BIGINT>(context, columns);
}

// from_days
DEFINE_UNARY_FN_WITH_IMPL(from_daysImpl, v) {
    // return 00-00-00 if the argument is negative or too large, according to MySQL
    DateValue dv{date::BC_EPOCH_JULIAN + v};
    if (!dv.is_valid()) {
        return DateValue{date::ZERO_EPOCH_JULIAN};
    }
    return dv;
}

StatusOr<ColumnPtr> TimeFunctions::from_days(FunctionContext* context, const Columns& columns) {
    return date_valid<TYPE_DATE>(
            VectorizedStrictUnaryFunction<from_daysImpl>::evaluate<TYPE_INT, TYPE_DATE>(VECTORIZED_FN_ARGS(0)));
}

// to_days
DEFINE_UNARY_FN_WITH_IMPL(to_daysImpl, v) {
    return v.julian() - date::BC_EPOCH_JULIAN;
}
DEFINE_TIME_UNARY_FN(to_days, TYPE_DATE, TYPE_INT);

// remove spaces at start and end, and if remained slice is "%Y-%m-%d", '-' means
// any char, then return true, set start to the first unspace char; else return false;
bool TimeFunctions::is_date_format(const Slice& slice, char** start) {
    char* ptr = slice.data;
    char* end = slice.data + slice.size;

    while (ptr < end && isspace(*ptr)) {
        ++ptr;
    }
    while (ptr < end && isspace(*(end - 1))) {
        --end;
    }

    *start = ptr;
    return (end - ptr) == 8 && ptr[0] == '%' && ptr[1] == 'Y' &&
           // slice[2] is default '-' and ignored actually
           ptr[3] == '%' && ptr[4] == 'm' &&
           // slice[5] is default '-' and ignored actually
           ptr[6] == '%' && ptr[7] == 'd';
}

// remove spaces at start and end, and if remained slice is "%Y-%m-%d %H:%i:%s", '-'/':' means
// any char, then return true, set start to the first unspace char; else return false;
bool TimeFunctions::is_datetime_format(const Slice& slice, char** start) {
    char* ptr = slice.data;
    char* end = slice.data + slice.size;

    while (ptr < end && isspace(*ptr)) {
        ++ptr;
    }
    while (ptr < end && isspace(*(end - 1))) {
        --end;
    }

    *start = ptr;
    return (end - ptr) == 17 && ptr[0] == '%' && ptr[1] == 'Y' &&
           // slice[2] is default '-' and ignored actually
           ptr[3] == '%' && ptr[4] == 'm' &&
           // slice[5] is default '-' and ignored actually
           ptr[6] == '%' && ptr[7] == 'd' &&
           // slice[8] is default ' ' and ignored actually
           ptr[9] == '%' && ptr[10] == 'H' &&
           // slice[11] is default ':' and ignored actually
           ptr[12] == '%' && ptr[13] == 'i' &&
           // slice[14] is default ':' and ignored actually
           ptr[15] == '%' && ptr[16] == 's';
}

// prepare for string format, if it is "%Y-%m-%d" or "%Y-%m-%d %H:%i:%s"
Status TimeFunctions::str_to_date_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr column = context->get_constant_column(1);
    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);

    // start point to the first unspace char in string format.
    char* start;
    if (is_date_format(slice, &start)) {
        auto* fc = new StrToDateCtx();
        fc->fmt_type = yyyycMMcdd;
        fc->fmt = start;
        context->set_function_state(scope, fc);
    } else if (is_datetime_format(slice, &start)) {
        auto* fc = new StrToDateCtx();
        fc->fmt_type = yyyycMMcddcHHcmmcss;
        fc->fmt = start;
        context->set_function_state(scope, fc);
    }
    return Status::OK();
}

// try to transfer content to date format based on "%Y-%m-%d",
// if successful, return result TimestampValue
// else take a uncommon approach to process this content.
template <bool isYYYYMMDD>
StatusOr<ColumnPtr> TimeFunctions::str_to_date_from_date_format(FunctionContext* context,
                                                                const starrocks::Columns& columns,
                                                                const char* str_format) {
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_DATETIME> result(size);

    TimestampValue ts;
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto fmt_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    for (size_t i = 0; i < size; ++i) {
        if (str_viewer.is_null(i)) {
            result.append_null();
        } else {
            const Slice& str = str_viewer.value(i);
            bool r = isYYYYMMDD ? ts.from_date_format_str(str.get_data(), str.get_size(), str_format)
                                : ts.from_datetime_format_str(str.get_data(), str.get_size(), str_format);
            if (r) {
                result.append(ts);
            } else {
                const Slice& fmt = fmt_viewer.value(i);
                str_to_date_internal(&ts, fmt, str, &result);
            }
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// uncommon approach to process string content, based on uncommon string format.
void TimeFunctions::str_to_date_internal(TimestampValue* ts, const Slice& fmt, const Slice& str,
                                         ColumnBuilder<TYPE_DATETIME>* result) {
    bool r = ts->from_uncommon_format_str(fmt.get_data(), fmt.get_size(), str.get_data(), str.get_size());
    if (r) {
        result->append(*ts);
    } else {
        result->append_null();
    }
}

// Try to process string content, based on uncommon string format
StatusOr<ColumnPtr> TimeFunctions::str_to_date_uncommon(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t size = columns[0]->size(); // minimum number of rows.
    ColumnBuilder<TYPE_DATETIME> result(size);

    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);
    auto fmt_viewer = ColumnViewer<TYPE_VARCHAR>(columns[1]);
    for (size_t i = 0; i < size; ++i) {
        if (str_viewer.is_null(i) || fmt_viewer.is_null(i)) {
            result.append_null();
        } else {
            const Slice& str = str_viewer.value(i);
            const Slice& fmt = fmt_viewer.value(i);
            TimestampValue ts;
            str_to_date_internal(&ts, fmt, str, &result);
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

Status TimeFunctions::ParseJodaState::prepare(std::string_view format_str) {
    joda = std::make_unique<joda::JodaFormat>();
    if (!joda->prepare(format_str)) {
        return Status::InvalidArgument(fmt::format("invalid datetime format: {}", format_str));
    }
    return {};
}

Status TimeFunctions::parse_joda_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_notnull_constant_column(1)) {
        return Status::NotSupported("The 2rd argument must be literal");
    }
    std::string_view format_str = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(1));
    auto state = std::make_unique<ParseJodaState>();
    RETURN_IF_ERROR(state->prepare(format_str));
    context->set_function_state(scope, state.release());

    return Status::OK();
}

Status TimeFunctions::parse_joda_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    auto* fc = reinterpret_cast<ParseJodaState*>(context->get_function_state(scope));
    if (fc) delete fc;
    return {};
}

StatusOr<ColumnPtr> TimeFunctions::parse_jodatime(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    size_t size = columns[0]->size(); // minimum number of rows.
    ColumnBuilder<TYPE_DATETIME> result(size);
    auto str_viewer = ColumnViewer<TYPE_VARCHAR>(columns[0]);

    auto state = reinterpret_cast<ParseJodaState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    auto& formatter = state->joda;
    RETURN_IF(!formatter, Status::InternalError("unprepared"));

    for (size_t i = 0; i < size; ++i) {
        if (str_viewer.is_null(i)) {
            result.append_null();
        } else {
            std::string_view str(str_viewer.value(i));

            DateTimeValue date_time_value;
            if (!formatter->parse(str, &date_time_value)) {
                if (context->state() && context->state()->get_sql_dialect() == "trino") {
                    std::string_view format_str =
                            ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(1));
                    return Status::InvalidArgument(fmt::format("Invalid format '{}' for '{}'", format_str, str));
                }
                result.append_null();
            } else {
                TimestampValue ts = TimestampValue::create(
                        date_time_value.year(), date_time_value.month(), date_time_value.day(), date_time_value.hour(),
                        date_time_value.minute(), date_time_value.second(), date_time_value.microsecond());
                result.append(ts);
            }
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// str_to_date, for the "str_to_date" in sql.
StatusOr<ColumnPtr> TimeFunctions::str_to_date(FunctionContext* context, const Columns& columns) {
    auto* ctx = reinterpret_cast<StrToDateCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (ctx == nullptr) {
        return str_to_date_uncommon(context, columns);
    } else if (ctx->fmt_type == yyyycMMcdd) { // for string format like "%Y-%m-%d"
        return str_to_date_from_date_format<true>(context, columns, ctx->fmt);
    } else { // for string format like "%Y-%m-%d %H:%i:%s"
        return str_to_date_from_date_format<false>(context, columns, ctx->fmt);
    }
}

// reclaim memory for str_to_date.
Status TimeFunctions::str_to_date_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* fc = reinterpret_cast<StrToDateCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (fc != nullptr) {
        delete fc;
    }

    return Status::OK();
}

DEFINE_UNARY_FN_WITH_IMPL(TimestampToDate, value) {
    return DateValue{timestamp::to_julian(value._timestamp)};
}

StatusOr<ColumnPtr> TimeFunctions::str2date(FunctionContext* context, const Columns& columns) {
    ASSIGN_OR_RETURN(ColumnPtr datetime, str_to_date(context, columns));
    return VectorizedStrictUnaryFunction<TimestampToDate>::evaluate<TYPE_DATETIME, TYPE_DATE>(datetime);
}

Status TimeFunctions::format_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr column = context->get_constant_column(1);
    auto* fc = new FormatCtx();
    context->set_function_state(scope, fc);

    if (column->only_null()) {
        fc->is_valid = false;
        return Status::OK();
    }

    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    fc->fmt = slice.to_string();

    fc->len = DateTimeValue::compute_format_len(slice.data, slice.size);
    if (fc->len >= 128) {
        fc->is_valid = false;
        return Status::OK();
    }

    if (fc->fmt == "%Y%m%d" || fc->fmt == "yyyyMMdd") {
        fc->fmt_type = TimeFunctions::yyyyMMdd;
    } else if (fc->fmt == "%Y-%m-%d" || fc->fmt == "yyyy-MM-dd") {
        fc->fmt_type = TimeFunctions::yyyy_MM_dd;
    } else if (fc->fmt == "%Y-%m-%d %H:%i:%s" || fc->fmt == "yyyy-MM-dd HH:mm:ss") {
        fc->fmt_type = TimeFunctions::yyyy_MM_dd_HH_mm_ss;
    } else if (fc->fmt == "%Y-%m") {
        fc->fmt_type = TimeFunctions::yyyy_MM;
    } else if (fc->fmt == "%Y%m") {
        fc->fmt_type = TimeFunctions::yyyyMM;
    } else if (fc->fmt == "%Y") {
        fc->fmt_type = TimeFunctions::yyyy;
    } else {
        fc->fmt_type = TimeFunctions::None;
    }

    fc->is_valid = true;
    return Status::OK();
}

Status TimeFunctions::format_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (fc != nullptr) {
        delete fc;
    }

    return Status::OK();
}

template <typename OP, LogicalType Type>
ColumnPtr date_format_func(const Columns& cols, size_t patten_size) {
    ColumnViewer<Type> viewer(cols[0]);

    size_t num_rows = viewer.size();
    ColumnBuilder<TYPE_VARCHAR> builder(num_rows);
    builder.data_column()->reserve(num_rows, num_rows * patten_size);

    for (int i = 0; i < num_rows; ++i) {
        if (viewer.is_null(i)) {
            builder.append_null();
            continue;
        }

        builder.append(OP::template apply<RunTimeCppType<Type>, RunTimeCppType<TYPE_VARCHAR>>(viewer.value(i)));
    }

    return builder.build(ColumnHelper::is_all_const(cols));
}

std::string format_for_yyyyMMdd(const DateValue& date_value) {
    int y, m, d, t;
    date_value.to_date(&y, &m, &d);
    char to[8];

    t = y / 100;
    to[0] = t / 10 + '0';
    to[1] = t % 10 + '0';

    t = y % 100;
    to[2] = t / 10 + '0';
    to[3] = t % 10 + '0';

    to[4] = m / 10 + '0';
    to[5] = m % 10 + '0';
    to[6] = d / 10 + '0';
    to[7] = d % 10 + '0';
    return {to, 8};
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(yyyyMMddImpl, v) {
    return format_for_yyyyMMdd((DateValue)v);
}

std::string format_for_yyyy_MM_dd_Impl(const DateValue& date_value) {
    return date_value.to_string();
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(yyyy_MM_dd_Impl, v) {
    auto d = (DateValue)v;
    return format_for_yyyy_MM_dd_Impl((DateValue)d);
}

std::string format_for_yyyyMMddHHmmssImpl(const TimestampValue& date_value) {
    return date_value.to_string(true);
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(yyyyMMddHHmmssImpl, v) {
    return format_for_yyyyMMddHHmmssImpl((TimestampValue)v);
}

std::string format_for_yyyy_MMImpl(const DateValue& date_value) {
    int y, m, d, t;
    date_value.to_date(&y, &m, &d);
    char to[7];
    t = y / 100;
    to[0] = t / 10 + '0';
    to[1] = t % 10 + '0';

    t = y % 100;
    to[2] = t / 10 + '0';
    to[3] = t % 10 + '0';

    to[4] = '-';
    to[5] = m / 10 + '0';
    to[6] = m % 10 + '0';
    return {to, 7};
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(yyyy_MMImpl, v) {
    return format_for_yyyy_MMImpl((DateValue)v);
}

std::string format_for_yyyyMMImpl(const DateValue& date_value) {
    int y, m, d, t;
    date_value.to_date(&y, &m, &d);
    char to[6];
    t = y / 100;
    to[0] = t / 10 + '0';
    to[1] = t % 10 + '0';

    t = y % 100;
    to[2] = t / 10 + '0';
    to[3] = t % 10 + '0';

    to[4] = m / 10 + '0';
    to[5] = m % 10 + '0';
    return {to, 6};
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(yyyyMMImpl, v) {
    return format_for_yyyyMMImpl((DateValue)v);
}

std::string format_for_yyyyImpl(const DateValue& date_value) {
    int y, m, d, t;
    date_value.to_date(&y, &m, &d);
    char to[4];
    t = y / 100;
    to[0] = t / 10 + '0';
    to[1] = t % 10 + '0';

    t = y % 100;
    to[2] = t / 10 + '0';
    to[3] = t % 10 + '0';
    return {to, 4};
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(yyyyImpl, v) {
    return format_for_yyyyImpl((DateValue)v);
}

DEFINE_STRING_UNARY_FN_WITH_IMPL(to_iso8601Impl, v) {
    return timestamp::to_string<true>(((TimestampValue)v).timestamp());
}

bool standard_format_one_row(const TimestampValue& timestamp_value, char* buf, const std::string& fmt) {
    int year, month, day, hour, minute, second, microsecond;
    timestamp_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &microsecond);
    DateTimeValue dt(TIME_DATETIME, year, month, day, hour, minute, second, microsecond);
    bool b = dt.to_format_string(fmt.c_str(), fmt.size(), buf);
    return b;
}

template <LogicalType Type>
StatusOr<ColumnPtr> standard_format(const std::string& fmt, int len, const starrocks::Columns& columns) {
    if (fmt.size() <= 0) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }

    auto ts_viewer = ColumnViewer<Type>(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    char buf[len];
    for (size_t i = 0; i < size; ++i) {
        if (ts_viewer.is_null(i)) {
            result.append_null();
        } else {
            auto ts = (TimestampValue)ts_viewer.value(i);
            bool b = standard_format_one_row(ts, buf, fmt);
            result.append(Slice(std::string(buf)), !b);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType Type>
StatusOr<ColumnPtr> do_format(const TimeFunctions::FormatCtx* ctx, const Columns& cols) {
    if (ctx->fmt_type == TimeFunctions::yyyyMMdd) {
        return date_format_func<yyyyMMddImpl, Type>(cols, 8);
    } else if (ctx->fmt_type == TimeFunctions::yyyy_MM_dd) {
        return date_format_func<yyyy_MM_dd_Impl, Type>(cols, 10);
    } else if (ctx->fmt_type == TimeFunctions::yyyy_MM_dd_HH_mm_ss) {
        return date_format_func<yyyyMMddHHmmssImpl, Type>(cols, 28);
    } else if (ctx->fmt_type == TimeFunctions::yyyy_MM) {
        return date_format_func<yyyy_MMImpl, Type>(cols, 7);
    } else if (ctx->fmt_type == TimeFunctions::yyyyMM) {
        return date_format_func<yyyyMMImpl, Type>(cols, 6);
    } else if (ctx->fmt_type == TimeFunctions::yyyy) {
        return date_format_func<yyyyImpl, Type>(cols, 4);
    } else {
        return standard_format<Type>(ctx->fmt, 128, cols);
    }
}

template <LogicalType Type>
void common_format_process(ColumnViewer<Type>* viewer_date, ColumnViewer<TYPE_VARCHAR>* viewer_format,
                           ColumnBuilder<TYPE_VARCHAR>* builder, int i) {
    if (viewer_format->is_null(i) || viewer_format->value(i).empty()) {
        builder->append_null();
        return;
    }

    auto format = viewer_format->value(i).to_string();
    if (format == "%Y%m%d" || format == "yyyyMMdd") {
        builder->append(format_for_yyyyMMdd(viewer_date->value(i)));
    } else if (format == "%Y-%m-%d" || format == "yyyy-MM-dd") {
        builder->append(format_for_yyyy_MM_dd_Impl(viewer_date->value(i)));
    } else if (format == "%Y-%m-%d %H:%i:%s" || format == "yyyy-MM-dd HH:mm:ss") {
        builder->append(format_for_yyyyMMddHHmmssImpl(viewer_date->value(i)));
    } else if (format == "%Y-%m") {
        builder->append(format_for_yyyy_MMImpl(viewer_date->value(i)));
    } else if (format == "%Y%m") {
        builder->append(format_for_yyyyMMImpl(viewer_date->value(i)));
    } else if (format == "%Y") {
        builder->append(format_for_yyyyImpl(viewer_date->value(i)));
    } else {
        char buf[128];
        auto ts = (TimestampValue)viewer_date->value(i);
        bool b = standard_format_one_row(ts, buf, viewer_format->value(i).to_string());
        builder->append(Slice(std::string(buf)), !b);
    }
}

// datetime_format
StatusOr<ColumnPtr> TimeFunctions::datetime_format(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (fc != nullptr && fc->is_valid) {
        return do_format<TYPE_DATETIME>(fc, columns);
    } else {
        auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);
        ColumnViewer<TYPE_DATETIME> viewer_date(columns[0]);
        ColumnViewer<TYPE_VARCHAR> viewer_format(columns[1]);

        ColumnBuilder<TYPE_VARCHAR> builder(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            if (viewer_date.is_null(i)) {
                builder.append_null();
                continue;
            }

            common_format_process(&viewer_date, &viewer_format, &builder, i);
        }

        return builder.build(all_const);
    }
}

// date_format
StatusOr<ColumnPtr> TimeFunctions::date_format(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (fc != nullptr && fc->is_valid) {
        return do_format<TYPE_DATE>(fc, columns);
    } else {
        int num_rows = columns[0]->size();
        ColumnViewer<TYPE_DATE> viewer_date(columns[0]);
        ColumnViewer<TYPE_VARCHAR> viewer_format(columns[1]);

        ColumnBuilder<TYPE_VARCHAR> builder(columns[0]->size());

        for (int i = 0; i < num_rows; ++i) {
            if (viewer_date.is_null(i)) {
                builder.append_null();
                continue;
            }

            common_format_process(&viewer_date, &viewer_format, &builder, i);
        }
        return builder.build(ColumnHelper::is_all_const(columns));
    }
}

Status TimeFunctions::jodatime_format_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr column = context->get_constant_column(1);
    auto* fc = new FormatCtx();
    context->set_function_state(scope, fc);

    if (column->only_null()) {
        fc->is_valid = false;
        return Status::OK();
    }

    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    fc->fmt = slice.to_string();

    fc->len = DateTimeValue::compute_format_len(slice.data, slice.size);
    if (fc->len >= 128) {
        fc->is_valid = false;
        return Status::OK();
    }

    if (fc->fmt == "yyyyMMdd") {
        fc->fmt_type = TimeFunctions::yyyyMMdd;
    } else if (fc->fmt == "yyyy-MM-dd") {
        fc->fmt_type = TimeFunctions::yyyy_MM_dd;
    } else if (fc->fmt == "yyyy-MM-dd HH:mm:ss") {
        fc->fmt_type = TimeFunctions::yyyy_MM_dd_HH_mm_ss;
    } else if (fc->fmt == "yyyy-MM") {
        fc->fmt_type = TimeFunctions::yyyy_MM;
    } else if (fc->fmt == "yyyyMM") {
        fc->fmt_type = TimeFunctions::yyyyMM;
    } else if (fc->fmt == "yyyy") {
        fc->fmt_type = TimeFunctions::yyyy;
    } else {
        fc->fmt_type = TimeFunctions::None;
    }

    fc->is_valid = true;
    return Status::OK();
}

Status TimeFunctions::jodatime_format_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (fc != nullptr) {
        delete fc;
    }

    return Status::OK();
}

bool joda_standard_format_one_row(const TimestampValue& timestamp_value, char* buf, std::string_view fmt) {
    int year, month, day, hour, minute, second, microsecond;
    timestamp_value.to_timestamp(&year, &month, &day, &hour, &minute, &second, &microsecond);
    DateTimeValue dt(TIME_DATETIME, year, month, day, hour, minute, second, microsecond);
    bool b = dt.to_joda_format_string(fmt.data(), fmt.size(), buf);
    return b;
}

template <LogicalType Type>
StatusOr<ColumnPtr> joda_standard_format(const std::string& fmt, int len, const starrocks::Columns& columns) {
    if (fmt.size() <= 0) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }

    auto ts_viewer = ColumnViewer<Type>(columns[0]);

    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> result(size);

    char buf[len];
    for (size_t i = 0; i < size; ++i) {
        if (ts_viewer.is_null(i)) {
            result.append_null();
        } else {
            auto ts = (TimestampValue)ts_viewer.value(i);
            bool b = joda_standard_format_one_row(ts, buf, fmt);
            result.append(Slice(std::string(buf)), !b);
        }
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType Type>
StatusOr<ColumnPtr> joda_format(const TimeFunctions::FormatCtx* ctx, const Columns& cols) {
    if (ctx->fmt_type == TimeFunctions::yyyyMMdd) {
        return date_format_func<yyyyMMddImpl, Type>(cols, 8);
    } else if (ctx->fmt_type == TimeFunctions::yyyy_MM_dd) {
        return date_format_func<yyyy_MM_dd_Impl, Type>(cols, 10);
    } else if (ctx->fmt_type == TimeFunctions::yyyy_MM_dd_HH_mm_ss) {
        return date_format_func<yyyyMMddHHmmssImpl, Type>(cols, 28);
    } else if (ctx->fmt_type == TimeFunctions::yyyy_MM) {
        return date_format_func<yyyy_MMImpl, Type>(cols, 7);
    } else if (ctx->fmt_type == TimeFunctions::yyyyMM) {
        return date_format_func<yyyyMMImpl, Type>(cols, 6);
    } else if (ctx->fmt_type == TimeFunctions::yyyy) {
        return date_format_func<yyyyImpl, Type>(cols, 4);
    } else {
        return joda_standard_format<Type>(ctx->fmt, 128, cols);
    }
}

template <LogicalType Type>
void common_joda_format_process(ColumnViewer<Type>* viewer_date, ColumnViewer<TYPE_VARCHAR>* viewer_format,
                                ColumnBuilder<TYPE_VARCHAR>* builder, int i) {
    if (viewer_format->is_null(i) || viewer_format->value(i).empty()) {
        builder->append_null();
        return;
    }

    std::string_view format = viewer_format->value(i);
    if (format == "yyyyMMdd") {
        builder->append(format_for_yyyyMMdd(viewer_date->value(i)));
    } else if (format == "yyyy-MM-dd") {
        builder->append(format_for_yyyy_MM_dd_Impl(viewer_date->value(i)));
    } else if (format == "yyyy-MM-dd HH:mm:ss") {
        builder->append(format_for_yyyyMMddHHmmssImpl(viewer_date->value(i)));
    } else if (format == "yyyy-MM") {
        builder->append(format_for_yyyy_MMImpl(viewer_date->value(i)));
    } else if (format == "yyyyMM") {
        builder->append(format_for_yyyyMMImpl(viewer_date->value(i)));
    } else if (format == "yyyy") {
        builder->append(format_for_yyyyImpl(viewer_date->value(i)));
    } else {
        char buf[128];
        auto ts = (TimestampValue)viewer_date->value(i);
        bool b = joda_standard_format_one_row(ts, buf, format);
        builder->append(Slice(std::string(buf)), !b);
    }
}

// format datetime using joda format
StatusOr<ColumnPtr> TimeFunctions::jodadatetime_format(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (fc != nullptr && fc->is_valid) {
        return joda_format<TYPE_DATETIME>(fc, columns);
    } else {
        auto [all_const, num_rows] = ColumnHelper::num_packed_rows(columns);
        ColumnViewer<TYPE_DATETIME> viewer_date(columns[0]);
        ColumnViewer<TYPE_VARCHAR> viewer_format(columns[1]);

        ColumnBuilder<TYPE_VARCHAR> builder(num_rows);
        for (int i = 0; i < num_rows; ++i) {
            if (viewer_date.is_null(i)) {
                builder.append_null();
                continue;
            }

            common_joda_format_process(&viewer_date, &viewer_format, &builder, i);
        }

        return builder.build(all_const);
    }
}

// format date using joda format
StatusOr<ColumnPtr> TimeFunctions::jodadate_format(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto* fc = reinterpret_cast<FormatCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (fc != nullptr && fc->is_valid) {
        return joda_format<TYPE_DATE>(fc, columns);
    } else {
        int num_rows = columns[0]->size();
        ColumnViewer<TYPE_DATE> viewer_date(columns[0]);
        ColumnViewer<TYPE_VARCHAR> viewer_format(columns[1]);

        ColumnBuilder<TYPE_VARCHAR> builder(columns[0]->size());

        for (int i = 0; i < num_rows; ++i) {
            if (viewer_date.is_null(i)) {
                builder.append_null();
                continue;
            }

            common_joda_format_process(&viewer_date, &viewer_format, &builder, i);
        }
        return builder.build(ColumnHelper::is_all_const(columns));
    }
}

StatusOr<ColumnPtr> TimeFunctions::date_to_iso8601(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    return date_format_func<yyyy_MM_dd_Impl, TYPE_DATE>(columns, 10);
}

StatusOr<ColumnPtr> TimeFunctions::datetime_to_iso8601(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    return date_format_func<to_iso8601Impl, TYPE_DATETIME>(columns, 26);
}

Status TimeFunctions::datetime_trunc_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_notnull_constant_column(0)) {
        return Status::InternalError("datetime_trunc just support const format value");
    }

    ColumnPtr column = context->get_constant_column(0);
    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    auto format_value = slice.to_string();

    // to lower case
    std::transform(format_value.begin(), format_value.end(), format_value.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    ScalarFunction function;
    if (format_value == "microsecond") {
        function = &TimeFunctions::datetime_trunc_microsecond;
    } else if (format_value == "millisecond") {
        function = &TimeFunctions::datetime_trunc_millisecond;
    } else if (format_value == "second") {
        function = &TimeFunctions::datetime_trunc_second;
    } else if (format_value == "minute") {
        function = &TimeFunctions::datetime_trunc_minute;
    } else if (format_value == "hour") {
        function = &TimeFunctions::datetime_trunc_hour;
    } else if (format_value == "day") {
        function = &TimeFunctions::datetime_trunc_day;
    } else if (format_value == "month") {
        function = &TimeFunctions::datetime_trunc_month;
    } else if (format_value == "year") {
        function = &TimeFunctions::datetime_trunc_year;
    } else if (format_value == "week") {
        function = &TimeFunctions::datetime_trunc_week;
    } else if (format_value == "quarter") {
        function = &TimeFunctions::datetime_trunc_quarter;
    } else {
        return Status::InternalError(
                "format value must in {microsecond, millisecond, second, minute, hour, day, month, year, week, "
                "quarter}");
    }

    auto fc = new DateTruncCtx();
    fc->function = function;
    context->set_function_state(scope, fc);
    return Status::OK();
}

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_microsecondImpl, v) {
    return v;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_microsecond, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_millisecondImpl, v) {
    TimestampValue result = v;
    result.trunc_to_millisecond();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_millisecond, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_secondImpl, v) {
    TimestampValue result = v;
    result.trunc_to_second();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_second, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_minuteImpl, v) {
    TimestampValue result = v;
    result.trunc_to_minute();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_minute, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_hourImpl, v) {
    TimestampValue result = v;
    result.trunc_to_hour();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_hour, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_dayImpl, v) {
    TimestampValue result = v;
    result.trunc_to_day();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_day, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_monthImpl, v) {
    TimestampValue result = v;
    result.trunc_to_month();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_month, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_yearImpl, v) {
    TimestampValue result = v;
    result.trunc_to_year();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_year, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_weekImpl, v) {
    int day_of_week = ((DateValue)v).weekday() + 1;
    TimestampValue result = v;
    result.trunc_to_week(-day_to_first[day_of_week]);
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_week, TYPE_DATETIME, TYPE_DATETIME, 1);

DEFINE_UNARY_FN_WITH_IMPL(datetime_trunc_quarterImpl, v) {
    TimestampValue result = v;
    result.trunc_to_quarter();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(datetime_trunc_quarter, TYPE_DATETIME, TYPE_DATETIME, 1);

StatusOr<ColumnPtr> TimeFunctions::datetime_trunc(FunctionContext* context, const Columns& columns) {
    auto ctc = reinterpret_cast<DateTruncCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    return ctc->function(context, columns);
}

Status TimeFunctions::datetime_trunc_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto fc = reinterpret_cast<DateTruncCtx*>(context->get_function_state(scope));
        delete fc;
    }

    return Status::OK();
}

Status TimeFunctions::date_trunc_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    if (!context->is_constant_column(0)) {
        return Status::InternalError("date_trunc just support const format value");
    }

    ColumnPtr column = context->get_constant_column(0);

    if (column->only_null()) {
        return Status::InternalError("format value can't be null");
    }

    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    auto format_value = slice.to_string();

    // to lower case
    std::transform(format_value.begin(), format_value.end(), format_value.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    ScalarFunction function;
    if (format_value == "day") {
        function = &TimeFunctions::date_trunc_day;
    } else if (format_value == "month") {
        function = &TimeFunctions::date_trunc_month;
    } else if (format_value == "year") {
        function = &TimeFunctions::date_trunc_year;
    } else if (format_value == "week") {
        function = &TimeFunctions::date_trunc_week;
    } else if (format_value == "quarter") {
        function = &TimeFunctions::date_trunc_quarter;
    } else {
        return Status::InternalError("format value must in {day, month, year, week, quarter}");
    }

    auto fc = new DateTruncCtx();
    fc->function = function;
    context->set_function_state(scope, fc);
    return Status::OK();
}

StatusOr<ColumnPtr> TimeFunctions::date_trunc_day(FunctionContext* context, const starrocks::Columns& columns) {
    return columns[1];
}

DEFINE_UNARY_FN_WITH_IMPL(date_trunc_monthImpl, v) {
    DateValue result = v;
    result.trunc_to_month();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(date_trunc_month, TYPE_DATE, TYPE_DATE, 1);

DEFINE_UNARY_FN_WITH_IMPL(date_trunc_yearImpl, v) {
    DateValue result = v;
    result.trunc_to_year();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(date_trunc_year, TYPE_DATE, TYPE_DATE, 1);

DEFINE_UNARY_FN_WITH_IMPL(date_trunc_weekImpl, v) {
    DateValue result = v;
    result.trunc_to_week();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(date_trunc_week, TYPE_DATE, TYPE_DATE, 1);

DEFINE_UNARY_FN_WITH_IMPL(date_trunc_quarterImpl, v) {
    DateValue result = v;
    result.trunc_to_quarter();
    return result;
}
DEFINE_TIME_UNARY_FN_EXTEND(date_trunc_quarter, TYPE_DATE, TYPE_DATE, 1);

StatusOr<ColumnPtr> TimeFunctions::date_trunc(FunctionContext* context, const Columns& columns) {
    auto ctc = reinterpret_cast<DateTruncCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    return ctc->function(context, columns);
}

Status TimeFunctions::date_trunc_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto fc = reinterpret_cast<DateTruncCtx*>(context->get_function_state(scope));
        delete fc;
    }

    return Status::OK();
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_years_since_epoch_dateImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    auto ts = TimestampValue::create(y, m, d, 0, 0, 0, 0);
    return years_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>(ts, TimeFunctions::unix_epoch);
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_years_since_epoch_datetimeImpl, v) {
    return years_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>(v, TimeFunctions::unix_epoch);
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_years_since_epoch_date(FunctionContext* context,
                                                                  const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_years_since_epoch_dateImpl>::evaluate<TYPE_DATE, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_years_since_epoch_datetime(FunctionContext* context,
                                                                      const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_years_since_epoch_datetimeImpl>::evaluate<TYPE_DATETIME, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_months_since_epoch_dateImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    auto ts = TimestampValue::create(y, m, d, 0, 0, 0, 0);
    return months_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>(ts, TimeFunctions::unix_epoch);
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_months_since_epoch_datetimeImpl, v) {
    return months_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>(v, TimeFunctions::unix_epoch);
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_months_since_epoch_date(FunctionContext* context,
                                                                   const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_months_since_epoch_dateImpl>::evaluate<TYPE_DATE, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_months_since_epoch_datetime(FunctionContext* context,
                                                                       const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_months_since_epoch_datetimeImpl>::evaluate<TYPE_DATETIME, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_days_since_epoch_dateImpl, v) {
    int y, m, d;
    ((DateValue)v).to_date(&y, &m, &d);
    auto ts = TimestampValue::create(y, m, d, 0, 0, 0, 0);
    return days_diffImpl::apply<TimestampValue, TimestampValue, int64_t>(ts, TimeFunctions::unix_epoch);
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_days_since_epoch_datetimeImpl, v) {
    return days_diffImpl::apply<TimestampValue, TimestampValue, int64_t>(v, TimeFunctions::unix_epoch);
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_days_since_epoch_date(FunctionContext* context,
                                                                 const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_days_since_epoch_dateImpl>::evaluate<TYPE_DATE, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_days_since_epoch_datetime(FunctionContext* context,
                                                                     const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_days_since_epoch_datetimeImpl>::evaluate<TYPE_DATETIME, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

DEFINE_UNARY_FN_WITH_IMPL(iceberg_hours_since_epoch_datetimeImpl, v) {
    return hours_diffImpl::apply<TimestampValue, TimestampValue, int64_t>(v, TimeFunctions::unix_epoch);
}

StatusOr<ColumnPtr> TimeFunctions::iceberg_hours_since_epoch_datetime(FunctionContext* context,
                                                                      const starrocks::Columns& columns) {
    return VectorizedStrictUnaryFunction<iceberg_hours_since_epoch_datetimeImpl>::evaluate<TYPE_DATETIME, TYPE_BIGINT>(
            VECTORIZED_FN_ARGS(0));
}

// used as start point of time_slice.
TimestampValue TimeFunctions::start_of_time_slice = TimestampValue::create(1, 1, 1, 0, 0, 0);
TimestampValue TimeFunctions::unix_epoch = TimestampValue::create(1970, 1, 1, 0, 0, 0);
std::string TimeFunctions::info_reported_by_time_slice = "time used with time_slice can't before 0001-01-01 00:00:00";
#undef DEFINE_TIME_UNARY_FN
#undef DEFINE_TIME_UNARY_FN_WITH_IMPL
#undef DEFINE_TIME_BINARY_FN
#undef DEFINE_TIME_BINARY_FN_WITH_IMPL
#undef DEFINE_TIME_STRING_UNARY_FN
#undef DEFINE_TIME_UNARY_FN_EXTEND

static int weekday_from_dow_abbreviation(const std::string& dow) {
    const int err_tag = -1, base = 1000;
    if (dow.length() < 2) {
        return err_tag;
    }
    switch (dow[0] + dow[1] * base) {
    case 'S' + 'u' * base:
        return (dow == "Su" || dow == "Sun" || dow == "Sunday") ? 0 : err_tag;
    case 'M' + 'o' * base:
        return (dow == "Mo" || dow == "Mon" || dow == "Monday") ? 1 : err_tag;
    case 'T' + 'u' * base:
        return (dow == "Tu" || dow == "Tue" || dow == "Tuesday") ? 2 : err_tag;
    case 'W' + 'e' * base:
        return (dow == "We" || dow == "Wed" || dow == "Wednesday") ? 3 : err_tag;
    case 'T' + 'h' * base:
        return (dow == "Th" || dow == "Thu" || dow == "Thursday") ? 4 : err_tag;
    case 'F' + 'r' * base:
        return (dow == "Fr" || dow == "Fri" || dow == "Friday") ? 5 : err_tag;
    case 'S' + 'a' * base:
        return (dow == "Sa" || dow == "Sat" || dow == "Saturday") ? 6 : err_tag;
    default:
        return err_tag;
    }
}

// next_day
StatusOr<ColumnPtr> TimeFunctions::next_day(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto* wdc = reinterpret_cast<WeekDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (wdc == nullptr) {
        return next_day_common(context, columns);
    }
    return next_day_wdc(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::next_day_wdc(FunctionContext* context, const Columns& columns) {
    auto* wdc = reinterpret_cast<WeekDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (wdc->dow_weekday == -2) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }
    auto time_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (time_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        TimestampValue time = time_viewer.value(row);
        int datetime_weekday = ((DateValue)time).weekday();
        auto date = (DateValue)timestamp_add<TimeUnit::DAY>(time, (6 + wdc->dow_weekday - datetime_weekday) % 7 + 1);
        result.append(date);
    }
    return date_valid<TYPE_DATE>(result.build(ColumnHelper::is_all_const(columns)));
}

StatusOr<ColumnPtr> TimeFunctions::next_day_common(FunctionContext* context, const Columns& columns) {
    auto time_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);
    auto dow_str = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (time_viewer.is_null(row) || dow_str.is_null(row)) {
            result.append_null();
            continue;
        }
        TimestampValue time = time_viewer.value(row);
        auto dow = dow_str.value(row).to_string();
        int dow_weekday = weekday_from_dow_abbreviation(dow);
        if (dow_weekday == -1) {
            return Status::InvalidArgument(dow + " not supported in next_day dow_string");
        }
        int datetime_weekday = ((DateValue)time).weekday();
        auto date = (DateValue)timestamp_add<TimeUnit::DAY>(time, (6 + dow_weekday - datetime_weekday) % 7 + 1);
        result.append(date);
    }
    return date_valid<TYPE_DATE>(result.build(ColumnHelper::is_all_const(columns)));
}

Status TimeFunctions::next_day_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || !context->is_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr column = context->get_constant_column(1);
    if (column->only_null()) {
        auto* wdc = new WeekDayCtx();
        wdc->dow_weekday = -2;
        context->set_function_state(scope, wdc);
        return Status::OK();
    }

    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    auto dow = slice.to_string();
    int dow_weekday = weekday_from_dow_abbreviation(dow);
    if (dow_weekday == -1) {
        return Status::InvalidArgument(dow + " not supported in next_day dow_string");
    }
    auto* wdc = new WeekDayCtx();
    wdc->dow_weekday = dow_weekday;
    context->set_function_state(scope, wdc);
    return Status::OK();
}

Status TimeFunctions::next_day_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* wdc = reinterpret_cast<WeekDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (wdc != nullptr) {
            delete wdc;
        }
    }

    return Status::OK();
}

// previous_day
StatusOr<ColumnPtr> TimeFunctions::previous_day(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    auto* wdc = reinterpret_cast<WeekDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (wdc == nullptr) {
        return previous_day_common(context, columns);
    }
    return previous_day_wdc(context, columns);
}

StatusOr<ColumnPtr> TimeFunctions::previous_day_wdc(FunctionContext* context, const Columns& columns) {
    auto* wdc = reinterpret_cast<WeekDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (wdc->dow_weekday == -2) {
        return ColumnHelper::create_const_null_column(columns[0]->size());
    }
    auto time_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (time_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        TimestampValue time = time_viewer.value(row);
        int datetime_weekday = ((DateValue)time).weekday();
        auto date = (DateValue)timestamp_add<TimeUnit::DAY>(time, -((6 + datetime_weekday - wdc->dow_weekday) % 7 + 1));
        result.append(date);
    }
    return date_valid<TYPE_DATE>(result.build(ColumnHelper::is_all_const(columns)));
}

StatusOr<ColumnPtr> TimeFunctions::previous_day_common(FunctionContext* context, const Columns& columns) {
    auto time_viewer = ColumnViewer<TYPE_DATETIME>(columns[0]);
    auto dow_str = ColumnViewer<TYPE_VARCHAR>(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (time_viewer.is_null(row) || dow_str.is_null(row)) {
            result.append_null();
            continue;
        }
        TimestampValue time = time_viewer.value(row);
        auto dow = dow_str.value(row).to_string();
        int dow_weekday = weekday_from_dow_abbreviation(dow);
        if (dow_weekday == -1) {
            return Status::InvalidArgument(dow + " not supported in previous_day dow_string");
        }
        int datetime_weekday = ((DateValue)time).weekday();
        auto date = (DateValue)timestamp_add<TimeUnit::DAY>(time, -((6 + datetime_weekday - dow_weekday) % 7 + 1));
        result.append(date);
    }
    return date_valid<TYPE_DATE>(result.build(ColumnHelper::is_all_const(columns)));
}

Status TimeFunctions::previous_day_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || !context->is_constant_column(1)) {
        return Status::OK();
    }

    ColumnPtr column = context->get_constant_column(1);
    if (column->only_null()) {
        auto* wdc = new WeekDayCtx();
        wdc->dow_weekday = -2;
        context->set_function_state(scope, wdc);
        return Status::OK();
    }

    Slice slice = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    auto dow = slice.to_string();
    int dow_weekday = weekday_from_dow_abbreviation(dow);
    if (dow_weekday == -1) {
        return Status::InvalidArgument(dow + " not supported in previous_day dow_string");
    }
    auto* wdc = new WeekDayCtx();
    wdc->dow_weekday = dow_weekday;
    context->set_function_state(scope, wdc);
    return Status::OK();
}

Status TimeFunctions::previous_day_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* wdc = reinterpret_cast<WeekDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (wdc != nullptr) {
            delete wdc;
        }
    }

    return Status::OK();
}

StatusOr<ColumnPtr> TimeFunctions::make_date(FunctionContext* context, const Columns& columns) {
    auto year_viewer = ColumnViewer<TYPE_INT>(columns[0]);
    auto date_viewer = ColumnViewer<TYPE_INT>(columns[1]);

    auto size = columns[0]->size();
    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (year_viewer.is_null(row) || date_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto date_of_year = date_viewer.value(row);
        if (date_of_year <= 0) {
            result.append_null();
            continue;
        }

        auto year = year_viewer.value(row);
        DateValue dv = DateValue::create(year, 1, 1);
        dv = dv.add<TimeUnit::DAY>(date_of_year - 1);
        if (!dv.is_valid()) {
            result.append_null();
            continue;
        }

        int tmp_year = 0, tmp_month = 0, tmp_day = 0;
        dv.to_date(&tmp_year, &tmp_month, &tmp_day);
        if (tmp_year != year) {
            result.append_null();
            continue;
        }

        result.append(dv);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

// date_diff
using DateDiffFunctionImpl = int64_t (*)(const TimestampValue&, const TimestampValue&);
const static std::unordered_map<std::string, std::pair<ScalarFunction, DateDiffFunctionImpl>> date_diff_func_map = {
        {"millisecond",
         {&TimeFunctions::milliseconds_diff, &milliseconds_diffImpl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"second", {&TimeFunctions::seconds_diff, &seconds_diffImpl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"minute", {&TimeFunctions::minutes_diff, &minutes_diffImpl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"hour", {&TimeFunctions::hours_diff, &hours_diffImpl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"day", {&TimeFunctions::days_diff, &days_diffImpl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"week", {&TimeFunctions::weeks_diff, &weeks_diffImpl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"month",
         {&TimeFunctions::months_diff_v2, &months_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"quarter",
         {&TimeFunctions::quarters_diff_v2, &quarters_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>}},
        {"year", {&TimeFunctions::years_diff_v2, &years_diff_v2Impl::apply<TimestampValue, TimestampValue, int64_t>}}};

StatusOr<ColumnPtr> TimeFunctions::datediff(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    if (context->is_notnull_constant_column(DateDiffCtx::TYPE_INDEX)) {
        auto ctx = reinterpret_cast<DateDiffCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        return ctx->function(context, {columns[DateDiffCtx::LHS_INDEX], columns[DateDiffCtx::RHS_INDEX]});
    }

    ColumnViewer<TYPE_VARCHAR> type_column(columns[DateDiffCtx::TYPE_INDEX]);
    ColumnViewer<TYPE_DATETIME> lv_column(columns[DateDiffCtx::LHS_INDEX]);
    ColumnViewer<TYPE_DATETIME> rv_column(columns[DateDiffCtx::RHS_INDEX]);
    auto size = columns[DateDiffCtx::TYPE_INDEX]->size();
    ColumnBuilder<TYPE_BIGINT> result(size);
    for (int row = 0; row < size; ++row) {
        if (lv_column.is_null(row) || rv_column.is_null(row) || type_column.is_null(row)) {
            result.append_null();
            continue;
        }
        TimestampValue l = (TimestampValue)lv_column.value(row);
        TimestampValue r = (TimestampValue)rv_column.value(row);
        auto type_str = type_column.value(row).to_string();
        transform(type_str.begin(), type_str.end(), type_str.begin(), ::tolower);
        auto iter = date_diff_func_map.find(type_str);
        if (iter != date_diff_func_map.end()) {
            result.append(iter->second.second(l, r));
        } else {
            return Status::InvalidArgument(
                    "unit of date_diff must be one of year/month/quarter/day/hour/minute/second/millisecond");
        }
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

Status TimeFunctions::datediff_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL || !context->is_notnull_constant_column(DateDiffCtx::TYPE_INDEX)) {
        return Status::OK();
    }
    ColumnPtr column = context->get_constant_column(DateDiffCtx::TYPE_INDEX);
    auto type_str = ColumnHelper::get_const_value<TYPE_VARCHAR>(column).to_string();
    transform(type_str.begin(), type_str.end(), type_str.begin(), ::tolower);
    auto iter = date_diff_func_map.find(type_str);
    if (iter == date_diff_func_map.end()) {
        return Status::InvalidArgument("type column should be one of day/hour/minute/second/millisecond");
    }
    auto fc = new TimeFunctions::DateDiffCtx();
    fc->function = iter->second.first;

    context->set_function_state(scope, fc);
    return Status::OK();
}

Status TimeFunctions::datediff_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto fc = reinterpret_cast<DateDiffCtx*>(context->get_function_state(scope));
        delete fc;
    }
    return Status::OK();
}

template <LogicalType DATE_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_last_day(FunctionContext* context, const Columns& columns) {
    ColumnViewer<DATE_TYPE> data_column(columns[0]);
    auto size = columns[0]->size();

    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row)) {
            result.append_null();
            continue;
        }

        DateValue date = (DateValue)data_column.value(row);
        date.set_end_of_month(); // default month
        result.append(date);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// last_day_date
StatusOr<ColumnPtr> TimeFunctions::last_day_date(FunctionContext* context, const Columns& columns) {
    return _last_day<TYPE_DATE>(context, columns);
}

// last_day
StatusOr<ColumnPtr> TimeFunctions::last_day(FunctionContext* context, const Columns& columns) {
    return _last_day<TYPE_DATETIME>(context, columns);
}

Status TimeFunctions::_error_date_part() {
    return Status::InvalidArgument("avaiable data_part parameter is year/month/quarter");
}

template <LogicalType DATE_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_last_day_with_format_const(std::string& format_content, FunctionContext* context,
                                                               const Columns& columns) {
    ColumnViewer<DATE_TYPE> data_column(columns[0]);
    auto size = columns[0]->size();

    ColumnBuilder<TYPE_DATE> result(size);
    if (format_content == "year") {
        for (int row = 0; row < size; ++row) {
            if (data_column.is_null(row)) {
                result.append_null();
                continue;
            }
            DateValue date = (DateValue)data_column.value(row);
            date.set_end_of_year();
            result.append(date);
        }
    } else if (format_content == "quarter") {
        for (int row = 0; row < size; ++row) {
            if (data_column.is_null(row)) {
                result.append_null();
                continue;
            }
            DateValue date = (DateValue)data_column.value(row);
            date.set_end_of_quarter();
            result.append(date);
        }
    } else if (format_content == "month") {
        for (int row = 0; row < size; ++row) {
            if (data_column.is_null(row)) {
                result.append_null();
                continue;
            }
            DateValue date = (DateValue)data_column.value(row);
            date.set_end_of_month();
            result.append(date);
        }
    } else {
        return _error_date_part();
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

template <LogicalType DATE_TYPE>
StatusOr<ColumnPtr> TimeFunctions::_last_day_with_format(FunctionContext* context, const Columns& columns) {
    ColumnViewer<DATE_TYPE> data_column(columns[0]);
    ColumnViewer<TYPE_VARCHAR> format_column(columns[1]);
    auto size = columns[0]->size();

    ColumnBuilder<TYPE_DATE> result(size);
    for (int row = 0; row < size; ++row) {
        if (data_column.is_null(row) || format_column.is_null(row)) {
            result.append_null();
            continue;
        }

        DateValue date = (DateValue)data_column.value(row);
        Slice format_content = format_column.value(row);
        std::transform(format_content.data, format_content.data + format_content.size, format_content.data,
                       [](auto x) { return std::tolower(x); });

        if (format_content == "year") {
            date.set_end_of_year();
        } else if (format_content == "quarter") {
            date.set_end_of_quarter();
        } else if (format_content == "month") {
            date.set_end_of_month();
        } else {
            return _error_date_part();
        }
        result.append(date);
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

// last_day_with_format
StatusOr<ColumnPtr> TimeFunctions::last_day_with_format(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    auto* state = reinterpret_cast<LastDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state->const_optional) {
        std::string format_content = state->optional_content;
        return _last_day_with_format_const<TYPE_DATETIME>(format_content, context, columns);
    }
    return _last_day_with_format<TYPE_DATETIME>(context, columns);
}

// last_day_date_with_format
StatusOr<ColumnPtr> TimeFunctions::last_day_date_with_format(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 2);
    auto* state = reinterpret_cast<LastDayCtx*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (state->const_optional) {
        std::string format_content = state->optional_content;
        return _last_day_with_format_const<TYPE_DATE>(format_content, context, columns);
    }
    return _last_day_with_format<TYPE_DATE>(context, columns);
}

Status TimeFunctions::last_day_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* state = new LastDayCtx();
    context->set_function_state(scope, state);

    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    state->const_optional = true;
    ColumnPtr column = context->get_constant_column(1);
    Slice optional = ColumnHelper::get_const_value<TYPE_VARCHAR>(column);
    std::transform(optional.data, optional.data + optional.size, optional.data, [](auto x) { return std::tolower(x); });
    if (!(optional == "year" || optional == "month" || optional == "quarter")) {
        return _error_date_part();
    }
    state->optional_content = optional;
    return Status::OK();
}

Status TimeFunctions::last_day_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    auto* ctx = reinterpret_cast<LastDayCtx*>(context->get_function_state(scope));
    if (ctx != nullptr) {
        delete ctx;
    }
    return Status::OK();
}

// Format a time value according to a format string
StatusOr<ColumnPtr> TimeFunctions::time_format(FunctionContext* context, const starrocks::Columns& columns) {
    if (columns.size() != 2) {
        return Status::InvalidArgument("FORMAT_TIME requires exactly 2 arguments");
    }

    const auto& time_column = columns[0];
    const auto& format_column = columns[1];

    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    auto time_viewer = ColumnViewer<TYPE_TIME>(time_column);
    auto format_viewer = ColumnViewer<TYPE_VARCHAR>(format_column);

    const size_t size = time_column->size();
    auto builder = ColumnBuilder<TYPE_VARCHAR>(size);

    for (size_t i = 0; i < size; ++i) {
        if (time_viewer.is_null(i) || format_viewer.is_null(i)) {
            builder.append_null();
            continue;
        }

        TimestampValue time_val;
        time_val.set_timestamp(time_viewer.value(i));
        std::string_view format_str = format_viewer.value(i);

        // Convert TimeValue to hours, minutes, seconds
        int year;
        int month;
        int day;
        int hours;
        int minutes;
        int seconds;
        int microseconds;
        time_val.to_timestamp(&year, &month, &day, &hours, &minutes, &seconds, &microseconds);

        std::stringstream result;
        bool in_format = false;

        for (size_t j = 0; j < format_str.size(); ++j) {
            char c = format_str[j];
            if (c == '%') {
                in_format = true;
                continue;
            }

            if (!in_format) {
                result << c;
                continue;
            }

            in_format = false;
            switch (c) {
            case 'H': // Hour (00-23)
                result << std::setfill('0') << std::setw(2) << hours;
                break;
            case 'h': // Hour (01-12)
                result << std::setfill('0') << std::setw(2) << (((hours % 12) == 0) ? 12 : (hours % 12));
                break;
            case 'i': // Minutes (00-59)
                result << std::setfill('0') << std::setw(2) << minutes;
                break;
            case 'S': // Seconds (00-59)
            case 's': // Seconds (00-59)
                result << std::setfill('0') << std::setw(2) << seconds;
                break;
            case 'f': // Microseconds (000000-999999)
                result << std::setfill('0') << std::setw(6) << microseconds;
                break;
            case 'p': // AM or PM
                result << (hours < 12 ? "AM" : "PM");
                break;
            default:
                result << '%' << c;
                break;
            }
        }

        if (in_format) {
            result << '%'; // Handle trailing %
        }

        builder.append(result.str());
    }

    return builder.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/TimeFunctions.inc"