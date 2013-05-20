CREATE OR REPLACE FUNCTION mongres_find(collection text, terms json, lim int, off int) RETURNS json[] AS $$
	if (/^admin/.test(collection)) {
		return [];
	}
	return [{"_id": 42}, {"_id": 43}];
$$ LANGUAGE plv8;
