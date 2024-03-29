import os

from flask import Flask, jsonify, redirect, render_template, request, url_for
import psycopg2

import db

app = Flask(__name__)

@app.before_first_request
def initialize():
    db.setup()

@app.route('/')
def home():
    link = url_for("people")
    # return f'Hello World! <a href="{link}">people</a>'
    return render_template("home.html", link=link)


@app.route('/people', methods=['GET', 'POST'])
def people():
    if request.method == 'POST':
        name = request.form['name']
        app.logger.info(f"got a name: {name}")
        with db.get_db_cursor(commit=True) as cur:
            cur.execute("insert into person (name) values (%s)", (name,))
        return redirect(url_for("people"))
    else:
        with db.get_db_cursor() as cur:
            cur.execute("SELECT * FROM person;")
            names = [record["name"] for record in cur]

        return render_template("people.html", names=names)


@app.route('/api/foo')
def api_foo():
    data = {
        "message": "hello, world",
        "isAGoodExample": False,
        "aList": [1, 2, 3],
        "nested": {
            "key": "value"
        }
    }
    return jsonify(data)


if __name__ == '__main__':
    pass