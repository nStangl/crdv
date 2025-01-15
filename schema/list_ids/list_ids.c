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


// Converts a text* type to char*
char *textToString(text* t) {
    char *str;

    if (t != NULL) {
        str = text_to_cstring(t);
    }
    else {
        str = palloc(1);
        str[0] = '\0';
    }

    return str;
}


// Char generation optimized for random inserts
int charBetweenRegular(int c1, int c2) {
    return (c1 + c2) / 2;
}


// Char generation optimized for appends
int charBetweenAppends(int c1, int c2) {
    return c1 + 1;
}


// Char generation optimized for prepends
int charBetweenPrepends(int c1, int c2) {
    return c2 - 1;
}


// Generates the list index between two other indexes, arg1 and arg2.
// Also receives as argument the char generator function.
Datum generateVirtualIndexBetween(PG_FUNCTION_ARGS, int(*charBetween)(int, int)) {
    text* arg1 = PG_ARGISNULL(0) ? NULL : PG_GETARG_TEXT_PP(0);
    text* arg2 = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_PP(1);

    char* p1 = textToString(arg1);
    char* p2 = textToString(arg2);
    int maxLen = Max(strlen(p1), strlen(p2));
    char* result = palloc(sizeof(char) * maxLen + 1);
    int i;

    for (i = 0; i < maxLen + 1; i++) {
        int c1 = i < strlen(p1) ? p1[i] : '!';
        int c2 = i < strlen(p2) ? p2[i] : 127;

        if (c2 - c1 > 1) {
            result[i] = charBetween(c1, c2);
            break;
        }
        else {
            result[i] = c1;
        }
    }

    result[i + 1] = '\0';

    pfree(p1);
    pfree(p2);
    PG_RETURN_TEXT_P(cstring_to_text(result));
}


PG_FUNCTION_INFO_V1(generateVirtualIndexBetweenRegular);

// Calls generateVirtualIndexBetween using the charBetweenRegular generator
Datum generateVirtualIndexBetweenRegular(PG_FUNCTION_ARGS) {
    return generateVirtualIndexBetween(fcinfo, charBetweenRegular);
}


PG_FUNCTION_INFO_V1(generateVirtualIndexBetweenAppends);

// Calls generateVirtualIndexBetween using the charBetweenAppends generator
Datum generateVirtualIndexBetweenAppends(PG_FUNCTION_ARGS) {
    return generateVirtualIndexBetween(fcinfo, charBetweenAppends);
}


PG_FUNCTION_INFO_V1(generateVirtualIndexBetweenPrepends);

// Calls generateVirtualIndexBetween using the charBetweenPrepends generator
Datum generateVirtualIndexBetweenPrepends(PG_FUNCTION_ARGS) {
    return generateVirtualIndexBetween(fcinfo, charBetweenPrepends);
}
