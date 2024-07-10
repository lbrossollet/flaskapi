# Let's write your code here!
from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def hello_world():
    return "Hello!"

@app.route("/morning")
def good_morning():
    return "Good Morning"
@app.route("/evening/<firstname>")
def evening(firstname):
    return f"Good evening, {firstname} "

@app.route("/greetings/<period_of_the_day>/<firstname>")
def greetings(period_of_the_day,firstname):
    return f"Good {period_of_the_day}, {firstname}"

@app.route("/add/<int:first>/<int:second>")
def add(first,second):
    return str( first + second )

@app.route("/afternoon")
def good_afternoon():
    firstname = request.args.get("firstname", "who are you")
    other_param=int(request.args.get('second','0'))
    other_param_2=int(request.args.get('third','0'))
    return f"Good afternoon {firstname} - {other_param} - {other_param+other_param_2}!"

@app.route("/substract")
def difference():
    first=int(request.args.get('first','0'))
    second=int(request.args.get('second','0'))
    return str(first-second)

@app.route("/hello")
def hello_api():
    return{"message": "Hello!", "hey": "I'm an API!"}