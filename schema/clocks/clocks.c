#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "varatt.h"
#include "utils/array.h"
#include <sys/time.h>
#include <math.h>
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "executor/executor.h"
#include "utils/typcache.h"
#include "funcapi.h"

PG_MODULE_MAGIC;


int64_t getCurrentTimeMillis() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + (int64_t)tv.tv_usec / 1000;
}


PG_FUNCTION_INFO_V1(vclock_lte);

// Computes whether the vclock v1 happens before v2
// (i.e., each element of v1 is <= the corresponding element of v2)
Datum vclock_lte(PG_FUNCTION_ARGS) {
    ArrayType* v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* v2 = PG_GETARG_ARRAYTYPE_P(1);

    Datum* v1Data;
    Datum* v2Data;
    bool* v1Null;
    bool* v2Null;
    int v1NumElements;
    int v2NumElements;
    deconstruct_array(v1, ARR_ELEMTYPE(v1), sizeof(int64_t), true, 'i', &v1Data, &v1Null, &v1NumElements);
    deconstruct_array(v2, ARR_ELEMTYPE(v2), sizeof(int64_t), true, 'i', &v2Data, &v2Null, &v2NumElements);

    // shouldn't happen
    if (v1NumElements > v2NumElements) {
        PG_RETURN_BOOL(false);
    }

    for (int i = 0; i < v1NumElements; i++) {
        if (DatumGetInt64(v1Data[i]) > DatumGetInt64(v2Data[i])) {
            PG_RETURN_BOOL(false);
        }
    }

    PG_RETURN_BOOL(true);
}


PG_FUNCTION_INFO_V1(vclock_max);

// Computes the pointwise max vclock of two vclocks
Datum vclock_max(PG_FUNCTION_ARGS) {
    ArrayType* v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType* v2 = PG_GETARG_ARRAYTYPE_P(1);
    Datum* v1Data;
    Datum* v2Data;
    bool* v1Null;
    bool* v2Null;
    int v1NumElements;
    int v2NumElements;

    deconstruct_array(v1, ARR_ELEMTYPE(v1), sizeof(int64_t), true, 'i', &v1Data, &v1Null, &v1NumElements);
    deconstruct_array(v2, ARR_ELEMTYPE(v2), sizeof(int64_t), true, 'i', &v2Data, &v2Null, &v2NumElements);

    int size = Max(v1NumElements, v2NumElements);
    Datum* result = palloc(sizeof(int64_t) * size);
    for (int i = 0; i < size; i++) {
        if (i < v1NumElements && i < v2NumElements) {
            result[i] = Max(DatumGetInt64(v1Data[i]), DatumGetInt64(v2Data[i]));
        }
        else if (i < v1NumElements) {
            result[i] = DatumGetInt64(v1Data[i]);
        }
        else {
            result[i] = DatumGetInt64(v2Data[i]);
        }
    }

    ArrayType* out = construct_array(result, size, ARR_ELEMTYPE(v1), sizeof(int64_t), true, 'd');
    PG_RETURN_ARRAYTYPE_P(out);
}


PG_FUNCTION_INFO_V1(next_hlc);

// Merges two physical timestamps and returns the result
Datum next_hlc(PG_FUNCTION_ARGS) {
    HeapTupleHeader v1 = PG_GETARG_HEAPTUPLEHEADER(0);
    bool isnull;
    int64_t currPTime = DatumGetInt64(GetAttributeByNum(v1, 1, &isnull));
    int64_t currLTime = DatumGetInt64(GetAttributeByNum(v1, 2, &isnull));
    int64_t currentTimeMillis = getCurrentTimeMillis();
    int64_t outPTime;
    int64_t outLTime;

    if (currentTimeMillis > currPTime) {
        outPTime = currentTimeMillis;
        outLTime = 1;
    }
    else {
        outPTime = currPTime;
        outLTime = currLTime + 1;
    }

    TupleDesc tupdesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(v1), -1);
    Datum values[] = {Int64GetDatum(outPTime), Int64GetDatum(outLTime)};
    bool nulls[] = {false, false};
    HeapTuple tuple = heap_form_tuple(tupdesc, values, nulls);
    ReleaseTupleDesc(tupdesc);

    PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
