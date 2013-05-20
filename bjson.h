#ifndef BJSON_H
#define BJSON_H

#include "lib/stringinfo.h"
#include "utils/builtins.h"

char *bson_to_json(StringInfo buf_in, const char *data);
void *json_to_bson(text *json);

#endif
