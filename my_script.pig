-- register datafu jar and create functions. 
register 'hdfs:///pigdata/datafu-pig-1.5.0.jar';

-- set the session timeout to be 15 mins
define Sessionize datafu.pig.sessions.Sessionize('15m');

REGISTER 'hdfs:///pigdata/piggybank-0.15.0.jar';

-- define iso to unix converter
DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

data = LOAD 'hdfs:///pigdata/2015_07_22_mktplace_shop_web_log_sample.log' USING org.apache.pig.piggybank.storage.CSVExcelStorage(' ') AS 
(timestamp:chararray,name:bytearray,client:chararray,backend:bytearray,request_time:bytearray,
backend_time:bytearray,response_time:bytearray,elb_status_code:bytearray,backend_status_code:bytearray,
received_bytes:bytearray,sent_bytes:bytearray,request:chararray,user_agent:bytearray,
ssl_cipher:bytearray,ssl_protocol:bytearray) parallel 3;

-- extract url from request field "GET HTTP://..." and ignore the port 
logs = FOREACH data GENERATE timestamp as iso_time, 
ISOToUnix(timestamp) AS unix_time:long,
STRSPLIT(client, ':', 2).$0 as user_ip, 
STRSPLIT(request, ' ', 3).$1 as visited_url;

-- sessionize
sessionized_logs = FOREACH (GROUP logs by user_ip) {
	ordered = ORDER logs BY iso_time;
    GENERATE FLATTEN(Sessionize(ordered)) AS (iso_time, unix_time, user_ip, visited_url, session_id);
}

-- aggregate page hits by session id  (Q1)                                                                                         
agg_sessionized_logs = FOREACH (GROUP sessionized_logs by session_id) 
GENERATE group as session_id, COUNT(sessionized_logs.visited_url) as total_pages_hit, 
sessionized_logs.user_ip;                                               

STORE agg_sessionized_logs INTO ' hdfs:///pigdata/q1_output/ ';

-- compute length of each session in minutes 
session_time = FOREACH (GROUP sessionized_logs BY session_id) {
	distinct_user_ip = DISTINCT sessionized_logs.user_ip;
	GENERATE group as session_id, BagToString(distinct_user_ip) as user_ip, 
	(MAX(sessionized_logs.unix_time) - MIN(sessionized_logs.unix_time))/1000.0/60.0 
	as session_length;
};

-- compute average session time in minutes (Q2)
session_stats = FOREACH (GROUP session_time ALL) {
	ordered = ORDER session_time BY session_length; 
    GENERATE AVG(ordered.session_length) as average_session_time; 
};

DUMP session_stats;
STORE session_stats INTO ' hdfs:///pigdata/q2_output/ ';

-- count the unique urls per session 
unique_sessionized_logs = FOREACH (GROUP sessionized_logs by session_id) {
	unique_url = DISTINCT sessionized_logs.visited_url;
    GENERATE COUNT(unique_url) as unique_visited_url;
};

-- calculate the average unique urls per session (Q3)
average_unique_url = FOREACH (GROUP unique_sessionized_logs ALL)
GENERATE AVG(unique_sessionized_logs.unique_visited_url);

DUMP average_unique_url;
STORE average_unique_url INTO ' hdfs:///pigdata/q3_output/ ';

-- group sessions by users 
grp_sessions_by_user = GROUP session_time by user_ip;

-- get the max session length for each user 
max_session_per_user = FOREACH grp_sessions_by_user {
	distinct_user_ip = DISTINCT session_time.user_ip;
	GENERATE BagToString(distinct_user_ip) as user_ip, 
    MAX(session_time.session_length) as max_session_time;
};

-- order user from the longest session length to the shortest
ordered_session_user = ORDER max_session_per_user by max_session_time DESC;

-- find the top 5 most engaged distinct users (Q4)
top5_engaged_users = Limit ordered_session_user 5;

DUMP top5_engaged_users;
STORE top5_engaged_users INTO ' hdfs:///pigdata/q4_output/ ';




