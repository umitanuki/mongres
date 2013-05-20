#include "postgres.h"

#include "utils/json.h"

#include "bson.h"

char *
bson_to_json(StringInfo *buf_in, const char *data)
{
	bison_iterator i;
	const char *key;
	int temp;
	bson_timestamp_t ts;
	char oidhex[25];
	bson scope;
	bson_iterator_from_buffer(&i, data);
	StringInfoData sidata, buf = &sidata;

	if (buf_in == NULL)
		initStringInfo(buf);
	else
		buf = buf_in;

	while (bson_iterator_next(&i))
	{
		bson_type t = bson_iterator_type(&i);
		if (t == 0)
			break;
		key = bson_iterator_key(&i);

		appendStringInfoString(buf, key);
		appendStringInfoChar(buf, ':');
		switch (t)
		{
			case BSON_DOUBLE:
				appendStringInfo(buf, "%f", bson_iterator_double(&i));
				break;
			case BSON_STRING:
				appendStringInfoString(buf, bson_iterator_string(&i));
				break;
			case BSON_OID:
				bson_oid_to_string(bson_iterator_oid(&i), oidhex);
				appendStringInfoString(buf, oidhex);
				break;
			case BSON_BOOL:
				appendStringInfoString(buf, bson_iterator_bool(&i) ? "true" : "false");
				break;
			case BSON_INT:
				appendStringInfo(buf, "%d", bson_iterator_int(&i));
				break;
			case BSON_LONG:
				appendStringInfo(buf, "%lld", (uint64_t) bson_iterator_long(&i));
				break;
			case BSON_DATE:
			case BSON_TIMESTAMP:
			case BSON_SYMBOL:
			case BSON_BINDATA:
			case BSON_UNDEFINED:
			case BSON_NULL:
			case BSON_REGEX:
			case BSON_CODE:
			case BSON_CODEWSCOPE:
				ereport(ERROR,
						(errmsg("unsupported bson type: %d", t)));
				break;
			default:
				elog(ERROR, "unknown bson type: %d", t);
				break;
		}
	}

	if (buf_in == NULL)
		return pstrdup(buf->data);
	else
		return buf->data;
}

static void
jbson_array_start(void *state)
{

}

static void
jbson_scalar(void *state)
{

}

static void
jbson_object_field_start(void *state)
{

}

void *
json_to_bson(text *json)
{
	bson	b;
	void   *result;
	JsonLexContext *lex = makeJsonLexContext(json, true);
	jsonSemAction sem;

	sem.semstate = (void *) &b;
	sem.array_start = jbson_array_start;
	sem.scalar = jbson_scalar;
	sem.object_field_start = jbson_object_field_start;

	bson_init(&b);
	pg_parse_json(lex, &sem);

	pfree(lex->strval->data);
	pfree(lex->strval);
	pfree(lex);

	bson_finish(&b);
	result = palloc(bson_size(&b));
	memcpy(result, b.data, bson_size(&b));
	bson_destroy(&b);

	return result;
}
