from flask_sqlalchemy import SQLAlchemy
from flask import Flask,request,jsonify
from flask_marshmallow import Marshmallow
import psycopg2.extras

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:8344862957@localhost/hosp'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
ma = Marshmallow(app)

conn = psycopg2.connect("postgresql://postgres:8344862957@localhost:5432/hosp")
cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

class Hospitals(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    hosp_name = db.Column(db.String(100), nullable=False)
    speciality = db.Column(db.String(100), nullable=False)
    city = db.Column(db.String(100), nullable=False)
    state = db.Column(db.String(100), nullable=False)
    latitude = db.Column(db.Float(), nullable=False)
    longitude = db.Column(db.Float(), nullable=False)

    def __init__(self, hosp_name, speciality, city, state, latitude, longitude):
        self.hosp_name = hosp_name
        self.speciality = speciality
        self.city = city
        self.state = state
        self.latitude = latitude
        self.longitude = longitude

db.create_all()

class TaskSchema(ma.Schema):
    class Meta:
        fields = ('id','hosp_name', 'speciality', 'city', 'state', 'latitude', 'longitude')

task_schema = TaskSchema()
tasks_schema = TaskSchema(many=True)

@app.route('/tasks', methods=['POST'])
def create_task():
    hosp_name = request.json['hosp_name']
    speciality = request.json['speciality']
    city = request.json['city']
    state = request.json['state']
    latitude = request.json['latitude']
    longitude = request.json['longitude']

    new_task = Hospitals(hosp_name, speciality, city, state, latitude, longitude)

    db.session.add(new_task)
    db.session.commit()

    if request.authorization and request.authorization.username == 'u' and request.authorization.password == 'p':
        return task_schema.jsonify(new_task)

@app.route('/tasks', methods=['GET'])
def get_tasks():
    all_tasks = Hospitals.query.all()
    result = tasks_schema.dump(all_tasks)
    if request.authorization and request.authorization.username == 'u' and request.authorization.password == 'p':
        return  jsonify(result)

@app.route('/tasks/<int:id>', methods=['GET'])
def get_task(id):
    task = Hospitals.query.get(id)
    if request.authorization and request.authorization.username == 'u' and request.authorization.password == 'p':
        return task_schema.jsonify(task)

@app.route('/tasks/near', methods=['GET'])
def hosp():
    cr.execute('select * from ( SELECT  *,( 3959 * acos( cos( radians(9.9252) ) * cos( radians( latitude ) ) * cos( radians( longitude ) - radians(78.1198) ) + '
               'sin( radians(9.9252) ) * sin( radians( latitude ) ) ) ) AS distance FROM hospitals ) h where id > 5;')
    hosp = cr.fetchall()
    hospital = [hosp]
    for row in hosp:
        hospital.append(dict(row))
        if request.authorization and request.authorization.username == 'u' and request.authorization.password == 'p':
            return jsonify(hosp)

@app.route('/tasks/<id>', methods=['PUT'])
def update_task(id):
    task = Hospitals.query.get(id)

    hosp_name = request.json['hosp_name']
    speciality = request.json['speciality']
    city = request.json['city']
    state = request.json['state']
    latitude = request.json['latitude']
    longitude = request.json['longitude']

    task.hosp_name = hosp_name
    task.speciality = speciality
    task.city = city
    task.state = state
    task.latitude = latitude
    task.longitude = longitude

    db.session.commit()

    if request.authorization and request.authorization.username == 'u' and request.authorization.password == 'p':
        return  task_schema.jsonify(task)

@app.route('/tasks/<id>', methods=['DELETE'])
def delete_task(id):
    task = Hospitals.query.get(id)
    db.session.delete(task)
    db.session.commit()

    if request.authorization and request.authorization.username == 'u' and request.authorization.password == 'p':
        return task_schema.jsonify(task)

if __name__ == "__main__":
    app.run(debug=True)