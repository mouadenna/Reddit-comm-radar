CREATE TABLE data_engineering_projects.reddit_comm_radar.topics (
  Topic BIGINT,
  Count BIGINT,
  Name STRING,
  Representation STRING,
  Representative_Docs STRING,
  _rescued_data STRING,
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
USING delta
COMMENT 'Created by the file upload UI'
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
---
CREATE VIEW reddit_comm_radar.sentiment_distribution (
  sentiment,
  post_count,
  avg_score,
  avg_comments,
  avg_post_score)
WITH SCHEMA COMPENSATION
AS SELECT 
    sentiment,
    COUNT(*) as post_count,
    AVG(sentiment_score) as avg_score,
    AVG(num_comments) as avg_comments,
    AVG(score) as avg_post_score
FROM posts
GROUP BY sentiment
ORDER BY post_count DESC

---
CREATE VIEW reddit_comm_radar.sentiment_distribution (
  sentiment,
  post_count,
  avg_score,
  avg_comments,
  avg_post_score)
WITH SCHEMA COMPENSATION
AS SELECT 
    sentiment,
    COUNT(*) as post_count,
    AVG(sentiment_score) as avg_score,
    AVG(num_comments) as avg_comments,
    AVG(score) as avg_post_score
FROM posts
GROUP BY sentiment
ORDER BY post_count DESC


---

CREATE VIEW reddit_comm_radar.top_keywords (
  keyword,
  occurrence_count,
  avg_score)
WITH SCHEMA COMPENSATION
AS SELECT 
    keyword,
    COUNT(*) as occurrence_count,
    AVG(score) as avg_score
FROM topic_keywords
GROUP BY keyword
ORDER BY occurrence_count DESC
LIMIT 100

---
CREATE VIEW reddit_comm_radar.topic_distribution (
  topic,
  topic_name,
  post_count,
  avg_probability,
  avg_comments,
  avg_post_score)
WITH SCHEMA COMPENSATION
AS SELECT 
    p.topic,
    t.Name as topic_name,
    COUNT(*) as post_count,
    AVG(p.topic_probability) as avg_probability,
    AVG(p.num_comments) as avg_comments,
    AVG(p.score) as avg_post_score
FROM posts p
LEFT JOIN topics t ON p.topic = t.Topic
GROUP BY p.topic, t.Name
ORDER BY post_count DESC

---

CREATE TABLE data_engineering_projects.reddit_comm_radar.topic_keywords (
  id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  topic_id VARCHAR(20),
  keyword VARCHAR(100),
  score FLOAT,
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT `topic_keywords_pk` PRIMARY KEY (`id`))
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')

---

CREATE TABLE data_engineering_projects.reddit_comm_radar.topics (
  Topic BIGINT,
  Count BIGINT,
  Name STRING,
  Representation STRING,
  Representative_Docs STRING,
  _rescued_data STRING,
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)
USING delta
COMMENT 'Created by the file upload UI'
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
