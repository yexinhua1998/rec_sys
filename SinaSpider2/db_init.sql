CREATE TABLE page_content(
	url TEXT PRIMARY KEY,
	title TEXT,
	content TEXT,
	keywords TEXT,
	tags TEXT,
	is_extracted BOOLEAN,
	docid Serial
);

CREATE INDEX docid_index ON page_content(docid);

CREATE TABLE url_to_be_get(
	url TEXT PRIMARY KEY
);

CREATE TABLE index_to_be_get(
	etime INTEGER,
	lid INTEGER PRIMARY KEY,
	page INTEGER
);

CREATE FUNCTION is_url_getted(_url TEXT) RETURNS BOOLEAN AS
$FUNC_BODY$
BEGIN 
	RETURN EXISTS(SELECT * FROM page_content WHERE url=_url);
END;
$FUNC_BODY$
LANGUAGE plpgsql;

/*判断url是否在准备爬取的队列里*/
CREATE FUNCTION is_url_to_be_get(_url TEXT)RETURNS BOOLEAN AS
$FUNC_BODY$
BEGIN
	RETURN EXISTS(SELECT * FROM url_to_be_get WHERE url=_url);
END;
$FUNC_BODY$
LANGUAGE plpgsql;

/*插入url到队列*/
CREATE FUNCTION insert_url_to_be_get(_url TEXT)RETURNS BOOLEAN AS
$FUNC_BODY$
BEGIN
	IF NOT EXISTS(SELECT url FROM url_to_be_get WHERE url=_url) THEN
		INSERT INTO url_to_be_get VALUES(_url);
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;
$FUNC_BODY$
LANGUAGE plpgsql;


CREATE FUNCTION insert_page_content
(_url TEXT,_title TEXT,_content TEXT,_keywords TEXT,_tags TEXT) 
RETURNS BOOLEAN AS
$FUNC_BODY$
BEGIN
	IF NOT EXISTS(SELECT * FROM page_content WHERE url=_url) THEN
		DELETE FROM url_to_be_get WHERE url=_url;
		INSERT INTO page_content VALUES(_url,_title,_content,_keywords,_tags,FALSE);
		RETURN TRUE;
	ELSE
		RETURN FALSE;
	END IF;
END;
$FUNC_BODY$
LANGUAGE plpgsql;
