--student学生信息表
CREATE TABLE IF NOT EXISTS PUBLIC.STUDENT (
	STUDID INTEGER,
	NAME VARCHAR,
	EMAIL VARCHAR,
	dob Date,
	PRIMARY KEY (STUDID, NAME))
WITH "template=replicated,atomicity=ATOMIC,cache_name=student";
-- WITH "template=student,atomicity=ATOMIC,cache_name=student";

CREATE INDEX IF NOT EXISTS STUDENT_NE_INDEX ON PUBLIC.STUDENT (NAME, EMAIL);

-- grade成绩表
CREATE TABLE IF NOT EXISTS PUBLIC.GRADE (
	STUDID INTEGER,
	grade DOUBLE,
	PRIMARY KEY (STUDID))
WITH "template=replicated,atomicity=ATOMIC,cache_name=grade";
