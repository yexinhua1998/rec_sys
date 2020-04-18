CREATE TABLE doc_vec(
	url TEXT,
	vec double precision[300],
	docid Integer
);


CREATE INDEX docVec_docid_index ON doc_vec(docid);
CREATE INDEX docVec_url_index ON doc_vec using HASH(url);

CREATE TABLE most_sim(
	docid1 Integer,
	docid2 Integer,
	sim double precision
);

CREATE INDEX most_sim_docid1_index ON most_sim using HASH(docid1);