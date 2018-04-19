from flask import Flask, render_template, request, redirect, url_for
import json
import os
from py2neo import Graph, Path, Node, Relationship
import datetime as dt
from dateutil.relativedelta import relativedelta

graph = Graph(password="root")

app = Flask(__name__)

keywords={
	'q1':'Enter "Author Name" to get all the tweets by that author: ',
	'q2':'Enter "Author Name" to fetch all the users mentioned by the author: ',
	'q3':'Click to fetch the top 20 most frequently co-occurring hashtags: ',
	'q4':'Enter a "Hashtag" retrieve the top 20 most frequently co-occurring user-mentions with the hashtag: ',
	'q5':'Enter a "Location" to retrieve all the tagged hashtags: ',
	'q6':'Click to fetch the top 5 user-user pairs where a user-user pair is ranked by the number of retweets: ',
	'q7':'Click to fetch the top 5 user-user pairs where a user-user pair is ranked by the number of replies.',
	'q8':'Enter a "User" to delete the tweets: ',
	'q1midsem':'Enter a user-mention(screen-name) to display the mention pairs: ',
	'q2midsem':'Enter a user-mention(screen-name) to display the hashtag pairs: '
}


def q1(keyword):
	query = """
			MATCH(x:Author {author:{keyword}})-[:AUTHOR_IN]-(y) 
			Return y.tweet_text AS `tweet_text`, x.author AS `author`
		"""
	return graph.run(query, keyword=keyword)

def q2(keyword):
	query = """
			MATCH(x: Author {author:{keyword}})-[:AUTHOR_IN]-(y) 
			MATCH(y)-[:MENTION_IN]-(z) 
			RETURN z.mention AS `mention`, x.author AS `author`
		"""
	return graph.run(query, keyword=keyword)

def q3():
	query = """
			MATCH  (x)-[:HASHTAG_IN]-()-[:HASHTAG_IN]-(y) 
			WHERE x.hashtag>y.hashtag
			return count(x.hashtag+y.hashtag) AS `counter`,x.hashtag AS `hashtag1`,y.hashtag AS `hashtag2`
			order by counter desc
			LIMIT 20
		"""
	return graph.run(query)

def q4(keyword):
	query = """ 
			MATCH(y:Hashtag) WHERE y.hashtag = {keyword}
			MATCH  (x)-[:MENTION_IN]-()-[:HASHTAG_IN]-(y) 
			return count(x.mention+y.hashtag) AS `counter`,x.mention AS `mention`,y.hashtag AS `hashtag`
			order by counter desc
			LIMIT 20
		"""
	return graph.run(query, keyword=keyword)

def q5(keyword):
	query = """
			MATCH(x:Location) WHERE x.location={keyword}
			MATCH(x)-[:LOCATION_IN]-()-[:HASHTAG_IN]-(y)
			Return y.hashtag AS `hashtag`, x.location AS `location`
		"""
	return graph.run(query, keyword=keyword)

def q6():
	query = """
			MATCH(s:Retweet)-[:RETWEET_IN]-(x)-[:AUTHOR_IN]-(z)
			WHERE s.author_id<>z.author_id
			return COUNT(s.author+z.author) AS `counter`, s.author AS `author1`, z.author AS `author2`
            ORDER BY counter DESC
			LIMIT 5
		"""
	return graph.run(query)

def q7():
	query = """
			MATCH(x:ReplyTweet)-[:REPLY_IN]-()-[:AUTHOR_IN]-(z)
			WHERE x.author_id<>z.author_id
			Return COUNT(x.author+z.author) AS `counter`, x.author AS `author1`, z.author AS `author2`
			ORDER BY counter DESC
			LIMIT 5
		"""
	return graph.run(query)

def q8(keyword):
	query = """
			MATCH(x:Author)-[:AUTHOR_IN]-(z) 
			WHERE x.author={keyword}
			DETACH DELETE z
		"""
	return graph.run(query, keyword=keyword)

def q1midsem(keyword):
	query="""
			MATCH(y:NewScreenName)-[:NEW_AUTHOR_SCREEN_NAME_IN]-(tweet)-[:NEW_MENTION_IN]-(z) WHERE z.mention = {keyword}
			RETURN z.mention AS `user_mention`, COUNT(y.author_screen_name) AS `counter`, COLLECT(tweet.tid) AS `tweet_id`, y.author_screen_name AS `mentioning_user`
			ORDER BY counter desc
			LIMIT 2
	"""
	return graph.run(query, keyword=keyword)

def q2midsem(keyword):
	query="""
			MATCH(x:NewMention) WHERE x.mention={keyword}
			MATCH(x)-[:NEW_MENTION_IN]-(tweet)-[:NEW_HASHTAG_IN]-(z)
			MATCH(tweet)-[:NEW_TYPE_IN]-(w) WHERE w.type='Tweet'
			RETURN COUNT(z.hashtag+x.mention) AS `counter`, x.mention AS `user_mention`, z.hashtag AS `hashtag`, COLLECT(tweet.tid) AS `tweet_id`
			ORDER BY counter desc
			LIMIT 3
	"""
	return graph.run(query, keyword=keyword)

@app.route('/')
def index():
	return render_template('index.html', var="Hey")

@app.route('/process/<questionLink>', methods=['GET'])
def process(questionLink):
	return render_template('process.html', display_text=keywords[questionLink], q_number=questionLink)

@app.route('/result', methods=['POST'])
def result():
	try:
		keyword = request.form['keyword']
	except:
		pass
	question_number = request.form['question_number']
	if(question_number=='q1'):
		resultset = q1(keyword)
	elif(question_number=='q2'):
		resultset = q2(keyword)
	elif(question_number=='q3'):
		resultset = q3()
	elif(question_number=='q4'):
		resultset = q4(keyword)
	elif(question_number=='q5'):
		resultset = q5(keyword)
	elif(question_number=='q6'):
		resultset = q6()
	elif(question_number=='q7'):
		resultset = q7()
	elif(question_number=='q8'):
		resultset = q8(keyword)
	elif(question_number=='q1midsem'):
		resultset = q1midsem(keyword)
	else:
		resultset = q2midsem(keyword)
	return render_template('results.html', resultset=resultset, question_number=question_number)


if __name__ == '__main__':
	app.run(debug=True)



