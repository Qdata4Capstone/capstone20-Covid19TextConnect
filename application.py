from flask import Flask
from flask import render_template, request
import json
import os
import config
import pickle
from backend.index import GensimIndex, ElasticSearchIndex
from backend.tokenizer import SciSpacyTokenizer

DEBUG=True

def format_result(doc, score):
    title = doc.title
    authors = ""
    for author in doc.metadata["authors"]:
        authors += author + ", "
    authors = authors[:-2]
    if authors == "":
        authors = "N/A"
    if "url" in doc.metadata:
        url = doc.metadata["url"]
    else:
        url = None

    return {"title": title, "authors": authors, "url": url}

def create_app():

    # This line required for AWS Elastic Beanstalk
    # Specify the template and static folders so that AWS EB can find them!
    # On your dashboard, make sure to set the static to something like: PATH = /static/  DIRECTORY = webapp/static
    # (only then will the CSS work)
    app = Flask(__name__, template_folder='frontend/templates', static_folder='frontend/static')

    try:
        print("Loading index...")
        indexer = ElasticSearchIndex.load_latest()
    except FileNotFoundError:
        indexer = ElasticSearchIndex()
        indexer.init()

    @app.route('/')
    @app.route('/index')
    def index():
      return render_template('index.html')

    @app.route('/about')
    def about():
      return render_template('about.html',
          title='About COVID-QA',
        )
    
    @app.route('/advanced')
    def advanced():
        return render_template('advanced.html')

    # Actually run search
    @app.route('/query', methods=['GET'])
    def query():
        data = request.args
        query = data["query"]
        results = indexer.query(query)

        formated_result = []
        for result in results:
            doc = result[0]
            score = result[1]
            if doc.title != "":
                formated_result.append(format_result(doc, score))
 
        return render_template("result.html", orig_query=query, results=formated_result)

    return app

# Needed for AWS EB
application = create_app()

# run the app.
if __name__ == "__main__":
    application.run(debug=DEBUG, host="0.0.0.0", port=7000)

