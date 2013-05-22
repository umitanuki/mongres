CREATE OR REPLACE FUNCTION mongres_find(collection text, terms json, lim int, skip int) RETURNS json[] AS $$
	if (/\.\$cmd$/.test(collection)) {
		// for now...
		return [];
	}
  var sql = "SELECT data FROM " + collection;

  var where_clause = plv8.find_function("mongres_where_clause");
  var where = where_clause(terms);

  sql += " " + where.sql;
  if (lim > -1 )
  {
    sql += "limit " + lim;
  }
  if (skip > 0)
  {
    sql += "offset " + skip;
  }


  try {
    plv8.subtransaction(function(){
      var plan = plv8.prepare(sql, where.types);
      rows = plan.execute(where.binds);
      plan.free();
    });
  }
  catch(err) {
      if (err=='Error: relation "' + collection + '" does not exist')
        {
        rows = []
        }
  }
  var ret = [ ];

  for (var i = 0; i < rows.length; i++) {
    ret.push(rows[i].data);
  }

  return ret;
$$ LANGUAGE plv8;

CREATE OR REPLACE FUNCTION mongres_find_in_obj(data json, key varchar) RETURNS
VARCHAR AS $$
  var obj = data;
  var parts = key.split('.');

  var part = parts.shift();
  while (part && (obj = obj[part]) !== undefined) {
    part = parts.shift();
  }

  return obj;
$$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION mongres_find_in_obj_int(data json, key varchar) RETURNS
INT AS $$
  var obj = data;
  var parts = key.split('.');

  var part = parts.shift();
  while (part && (obj = obj[part]) !== undefined) {
    part = parts.shift();
  }

  return Number(obj);
$$ LANGUAGE plv8 IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION mongres_find_in_obj_exists(data json, key varchar) RETURNS
BOOLEAN AS $$
  var obj = data;
  var parts = key.split('.');

  var part = parts.shift();
  while (part && (obj = obj[part]) !== undefined) {
    part = parts.shift();
  }

  return (obj === undefined ? 'f' : 't');
$$ LANGUAGE plv8 IMMUTABLE STRICT;
CREATE OR REPLACE FUNCTION mongres_where_clause (terms json) RETURNS
VARCHAR AS $$
  var sql = '';
  var c = [ ];
  var t = [ ];
  var b = [ ];

  var count = 1;
  
  function build_clause (key, value, type) {
    var clauses = [ ],
        binds   = [ ],
        types   = [ ];

    if (typeof(value) === 'object') {
      if (key === '$or') {
        var tclauses = [ ];

        for (var i = 0; i < value.length; i++) {
          var ret  = build_clause(Object.keys(value[i])[0], value[i][Object.keys(value[i])[0]]);

          tclauses = tclauses.concat(ret.clauses);
          binds    = binds.concat(ret.binds);
          types    = types.concat(ret.types);
        }

        clauses.push('( ' + tclauses.join(' OR ') + ' )');
      } else {
        var keys = Object.keys(value);

        for (var i = 0; i < keys.length; i++) {
          var ret;
          if (keys[i] === '$gt') {
            ret = build_clause(key, value[keys[i]], '>');
          } else if (keys[i] === '$lt') {
            ret = build_clause(key, value[keys[i]], '<');
          } else if (keys[i] === '$gte') {
            ret = build_clause(key, value[keys[i]], '>=');
          } else if (keys[i] === '$lte') {
            ret = build_clause(key, value[keys[i]], '<=');
          } else if (keys[i] === '$exists') {
            ret = build_clause(key, value[keys[i]], 'exists');
          }

          clauses = clauses.concat(ret.clauses);
          binds   = binds.concat(ret.binds);
          types   = types.concat(ret.types);
        }
      }
    } else {
      type = type || '=';
      var lval;

      if (type === 'exists') {
        clauses.push("mongres_find_in_obj_exists(data, '" + key + "') = $" + count);
        types.push('boolean');
        value = value ? 't' : 'f';
      } else {
        switch (typeof(value)) {
          case 'number':
          clauses.push("mongres_find_in_obj_int(data, '" + key + "') " + type + " $" + count);
          types.push('int');
          break;

          case 'string':
          clauses.push("mongres_find_in_obj(data, '" + key + "') " + type + " $" + count);
          types.push('varchar');
          break;

          default:
          console.log("unknown type: " + typeof(value));
        }
      }

      binds.push(value);

      count++;
    }

    return { clauses: clauses, binds: binds, types: types };
  }
    
  if (terms !== undefined) {
    var obj = terms; //JSON.parse(terms);
    var keys = Object.keys(obj);

    for (var i = 0; i < keys.length; i ++) {
      var ret = build_clause(keys[i], obj[keys[i]]);
      c = c.concat(ret.clauses);
      b = b.concat(ret.binds);
      t = t.concat(ret.types);
    }
    
    if (c.length) {
      sql += " WHERE ";
      
      sql += c.join(" AND ");
    }
  }

//  return JSON.stringify({ types: t, binds: b, sql: sql });
  return { types: t, binds: b, sql: sql };
$$ LANGUAGE plv8 STRICT;


CREATE OR REPLACE FUNCTION mongres_insert(collection text, docs json) RETURNS int AS $$
	var names = collection.split(".");
	var schema = names[0], table = names[1];

	var count = plv8.execute(
		"SELECT count(*) FROM pg_namespace WHERE nspname = $1",
		[schema]).shift().count;
	if (count == 0) {
		plv8.execute(
			"CREATE SCHEMA " + schema);
	}

	var count = plv8.execute(
		"SELECT count(*) FROM pg_class" +
		" INNER JOIN pg_namespace ON pg_namespace.oid = relnamespace" +
		" WHERE nspname = $1 AND relname = $2", [schema, table]).shift().count;
	if (count == 0) {
		plv8.execute(
			"CREATE TABLE " + collection +
			" (id varchar primary key, data json)");
	}

	for (var i = 0; i < docs.length; i++) {
		var elm = docs[i];
		// Since BSON misses top-level Array definition, elm is always an
		// object, but could be an array. Sigh, just convert it...
		var ary = [elm];
		if ("0" in elm) {
			for (var key in elm) {
				ary[key - 0] = elm[key];
			}
		}

		for (var j = 0; j < ary.length; j++) {
			var doc = ary[j];
			var id = doc._id;
			if (id == void 0) {
				id = plv8.execute(
					"SELECT substring(md5(random()::text), 1, 24) as id").shift().id;
				doc._id = id;
			}
			plv8.execute(
				"INSERT INTO " + collection + " VALUES($1, $2)", [id, doc]);
		}
	}

	// OidFunctionCall does not like NULL output
	return 0;
$$ LANGUAGE plv8;
