CREATE TABLE page_content(
	url TEXT PRIMARY KEY,
	content TEXT,
	is_extracted BOOLEAN
);


CREATE FUNCTION is_url_getted(_url TEXT) RETURNS BOOLEAN AS
$FUNC_BODY$
BEGIN 
	RETURN exists(SELECT * FROM page_content WHERE url=_url);
END;
$FUNC_BODY$
LANGUAGE plpgsql;

CREATE FUNCTION insert_page_content(_url TEXT,_content TEXT) RETURNS BOOLEAN AS
$FUNC_BODY$
BEGIN
	INSERT INTO page_content VALUES(_url,_content,FALSE);
	RETURN TRUE;
END;
$FUNC_BODY$
LANGUAGE plpgsql;