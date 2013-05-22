#include "postgres.h"

#include "utils/jsonapi.h"

#include "bson.h"

#include "bjson.h"

static char *bson_to_json_recurse(StringInfo buf_in,
					const char *data, bool is_object);

static char *
quote_string(const char *s)
{
	char	   *result = palloc(strlen(s) * 2 + 3);
	char	   *r = result;

	*r++ = '"';
	while (*s)
	{
		if (*s == '"')
			*r++ = '\\';
		*r++ = *s;
		s++;
	}
	*r++ = '"';
	*r++ = '\0';

	return result;
}

char *
bson_to_json(StringInfo buf_in, const char *data)
{
	StringInfoData sidata, *buf = &sidata;

	if (buf_in == NULL)
		initStringInfo(buf);
	else
		buf = buf_in;

	appendStringInfoChar(buf, '{');
	bson_to_json_recurse(buf, data, true);
	appendStringInfoChar(buf, '}');

	if (buf_in == NULL)
		return pstrdup(buf->data);
	else
		return buf->data;
}

static char *
bson_to_json_recurse(StringInfo buf, const char *data, bool is_object)
{
	bson_iterator i;
	const char *key;
	char oidhex[25];
	char *str;
	bool first = true;

	bson_iterator_from_buffer(&i, data);

	while (bson_iterator_next(&i))
	{
		bson_type t = bson_iterator_type(&i);
		if (t == 0)
			break;

		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;

		if (is_object)
		{
			key = bson_iterator_key(&i);

			appendStringInfo(buf, "\"%s\"", key);
			appendStringInfoChar(buf, ':');
		}
		switch (t)
		{
			case BSON_DOUBLE:
				appendStringInfo(buf, "%f", bson_iterator_double(&i));
				break;
			case BSON_STRING:
				str = quote_string(bson_iterator_string(&i));
				appendStringInfoString(buf, str);
				break;
			case BSON_OID:
				bson_oid_to_string(bson_iterator_oid(&i), oidhex);
				str = quote_string(oidhex);
				appendStringInfoString(buf, str);
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
			case BSON_NULL:
				appendStringInfoString(buf, "null");
				break;
			case BSON_OBJECT:
				appendStringInfoChar(buf, '{');
				bson_to_json_recurse(buf, bson_iterator_value(&i), true);
				appendStringInfoChar(buf, '}');
				break;
			case BSON_ARRAY:
				appendStringInfoChar(buf, '[');
				bson_to_json_recurse(buf, bson_iterator_value(&i), false);
				appendStringInfoChar(buf, ']');
				break;
			case BSON_DATE:
			case BSON_TIMESTAMP:
			case BSON_SYMBOL:
			case BSON_BINDATA:
			case BSON_UNDEFINED:
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

	return buf->data;
}

typedef struct json_to_bson_state {
	bson	   *bson;
	char	   *fname;
	JsonLexContext *lex;
} json_to_bson_state;

static void
jbson_object_start(void *state)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;

	if (_state->lex->lex_level == 0)
		return;

	if (_state->fname == NULL)
		elog(ERROR, "unsupported json structure");

	bson_append_start_object(_state->bson, _state->fname);
}

static void
jbson_object_end(void *state)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;

	if (_state->lex->lex_level == 0)
		return;
	bson_append_finish_object(_state->bson);
}

static void
jbson_array_start(void *state)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;

	if (_state->lex->lex_level == 0)
		return;

	if (_state->fname == NULL)
		elog(ERROR, "unsupported json structure");

	bson_append_start_array(_state->bson, _state->fname);
}

static void
jbson_array_end(void *state)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;

	if (_state->lex->lex_level == 0)
		return;
	bson_append_finish_array(_state->bson);
}

static void
jbson_object_field_start(void *state, char *fname, bool isnull)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;

	if (isnull)
		elog(ERROR, "fname cannot be null");

	_state->fname = fname;
}

static void
jbson_array_element_start(void *state, bool isnull)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;

	_state->fname = "";
}

static void
jbson_scalar(void *state, char *token, JsonTokenType tokentype)
{
	json_to_bson_state *_state = (json_to_bson_state *) state;
	double fval;

	if (_state->fname == NULL)
	{
		elog(ERROR, "unsupported json structure");
	}

	switch (tokentype)
	{
		case JSON_TOKEN_STRING:
			bson_append_string(_state->bson, _state->fname, token);
			break;
		case JSON_TOKEN_NUMBER:
			sscanf(token, "%lf", &fval);
			bson_append_double(_state->bson, _state->fname, fval);
			break;
		case JSON_TOKEN_TRUE:
			bson_append_bool(_state->bson, _state->fname, true);
			break;
		case JSON_TOKEN_FALSE:
			bson_append_bool(_state->bson, _state->fname, false);
			break;
		case JSON_TOKEN_NULL:
			bson_append_null(_state->bson, _state->fname);
			break;
		default:
			elog(ERROR, "unexpected token type: %d", tokentype);
			break;
	}

	_state->fname = NULL;
}

void *
json_to_bson(text *json)
{
	bson	b;
	void   *result;
	JsonLexContext *lex = makeJsonLexContext(json, true);
	jsonSemAction sem;
	json_to_bson_state state;

	memset(&state, 0, sizeof(json_to_bson_state));
	state.bson = &b;
	state.lex = lex;

	memset(&sem, 0, sizeof(sem));
	sem.semstate = (void *) &state;
	sem.object_start = jbson_object_start;
	sem.object_end = jbson_object_end;
	sem.array_start = jbson_array_start;
	sem.array_end = jbson_array_end;
	sem.array_element_start = jbson_array_element_start;
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
