import os
import json
from py2neo import Graph, Path, Node, Relationship

graph = Graph(password="root")
# graph.run("MATCH(n) RETURN n;")

int_attr = ['quote_count', 'like_count', 'reply_count', 'sentiment', 'retweet_count']
list_attr = ['hashtags', 'mentions', 'keywords_processed_list', 'url_list']

def add_data(data_list):

	query_u="""
			FOREACH(tweet in {data_list}|
				MERGE (Node: NewTweet
					{
						tid: tweet.tid,
						tweet_text : tweet.tweet_text
					})

				FOREACH(author_screen_name in tweet.author_screen_name|
					MERGE(ChildNode1: NewScreenName
						{
							author_screen_name : author_screen_name
						})
					MERGE (Node)-[:NEW_AUTHOR_SCREEN_NAME_IN]->(ChildNode1)
				)

				FOREACH(mention in tweet.mentions |
					MERGE (ChildNode3: NewMention
						{
							mention : mention
						})
					MERGE (Node)-[:NEW_MENTION_IN]->(ChildNode3)
				)

				FOREACH(hashtag in tweet.hashtags |
					MERGE (ChildNode1: NewHashtag
						{
							hashtag : hashtag
						})
					MERGE (Node)-[:NEW_HASHTAG_IN]->(ChildNode1)
				)

				FOREACH(type in tweet.type |
					MERGE (ChildNode2: NewType
						{
							type : type
						})
					MERGE (Node)-[:NEW_TYPE_IN]->(ChildNode2)
				)
			)
	"""

	query="""
			FOREACH(tweet IN {data_list}|
				MERGE (Node:Tweet 
						{
							tid : tweet.tid,
							quote_count : tweet.quote_count,
							like_count : tweet.like_count,
							sentiment : tweet.sentiment,
							retweet_count : tweet.retweet_count,
							tweet_text : tweet.tweet_text
						})

				FOREACH(keyword in tweet.keywords_processed_list |
					MERGE (ChildNode1: Keyword
						{
							keyword : keyword
						})
					MERGE (Node)-[:KEYWORD_IN]->(ChildNode1)
				)

				FOREACH(hashtag in tweet.hashtags |
					MERGE (ChildNode2: Hashtag
						{
							hashtag : hashtag
						})
					MERGE (Node)-[:HASHTAG_IN]->(ChildNode2)
				)

				FOREACH(mention in tweet.mentions |
					MERGE (ChildNode3: Mention
						{
							mention : mention
						})
					MERGE (Node)-[:MENTION_IN]->(ChildNode3)
				)

				FOREACH(location in tweet.location |
					MERGE (ChildNode4: Location
						{
							location : location
						})
					MERGE (Node)-[:LOCATION_IN]->(ChildNode4)
				)

				FOREACH(type in tweet.type |
					MERGE (ChildNode5: Type
						{
							type : type
						})
					MERGE (Node)-[:TYPE_IN]->(ChildNode5)
				)

				FOREACH(author in tweet.author |
					MERGE (ChildNode6: Author
						{
							author : author,
							author_id : tweet.author_id
						})
					MERGE (Node)-[:AUTHOR_IN]->(ChildNode6)
				)

				FOREACH(author in tweet.author_screen_name |
					MERGE (ChildNode7: AuthorScreenName
						{
							author_screen_name : author
						})
					MERGE (Node)-[:AUTHOR_SCREEN_IN]->(ChildNode7)
				)

				FOREACH(date in tweet.date |
					MERGE (ChildNode8: Date
						{
							date : date,
							datetime : tweet.datetime
						})
					MERGE (Node)-[:DATE_IN]->(ChildNode8)
				)

				FOREACH(verified in tweet.verified |
					MERGE (ChildNode9: Verified
						{
							verified : verified
						})
					MERGE (Node)-[:IS_VERIFIED]->(ChildNode9)
				)
				
			)

			UNWIND {data_list} AS tweet
				WITH tweet
				WITH COLLECT(tweet.retweet_source_id) AS retweet, tweet
				UNWIND retweet AS retweet_id
				WITH retweet_id, retweet, tweet
				MATCH(ParentNode: Tweet) WHERE ParentNode.tid=retweet_id
					MERGE (ChildNode1: Retweet
						{
							author : tweet.author,
							author_id : tweet.author_id
						})
					CREATE (ParentNode)-[:RETWEET_IN]->(ChildNode1)

			UNWIND {data_list} AS tweet
				WITH tweet
				WITH COLLECT(tweet.replyto_source_id) AS reply, tweet
				UNWIND reply AS reply_id
				WITH reply_id, reply, tweet
				MATCH(ParentNode: Tweet) WHERE ParentNode.tid=reply_id
					MERGE (ChildNode1: ReplyTweet
						{
							author : tweet.author,
							author_id : tweet.author_id
						})
					CREATE (ParentNode)-[:REPLY_IN]->(ChildNode1)

	"""
	graph.run(query_u, data_list=data_list)

def read_data():
	all_files = os.listdir()
	for filename in all_files:
		filename_s = filename.split('.')
		if(filename_s[-1]=="json"):
			with open(filename) as json_data:
			    d_json = json.load(json_data)
			    value_list=[]
			    for key, value in d_json.items():
			    	value_list.append(value)
			    add_data(value_list)
			    print(filename)

def read_q_data():
	
	with open('dataset.json') as json_data:
	    d_json = json.load(json_data)
	    value_list=[]
	    for key, value in d_json.items():
	    	value_list.append(value)
	    add_data(value_list)
	    print('dataset.json')

if __name__ == '__main__':
	read_data()
	read_q_data()
