CREATE VIEW recommended_title AS
SELECT docid1,docid2,title,sim
FROM page_content,most_sim
WHERE docid2=docid;