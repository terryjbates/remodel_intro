#!/usr/bin/env python3

import sys
import os
import rethinkdb as r
import remodel.utils
import remodel.connection
from remodel.models import Model
import flask


app = flask.Flask(__name__)

try:
    conn = r.connect(db='fleet')
except r.ReqlDriverError as db_connect_ex:
    print("Exception connecting to RethinkDB instance: {}".format(db_connect_ex))
    sys.exit()

table_list_cur = r.table_list()

try:
    table_list_cur.run(conn)
except r.ReqlRuntimeError as table_list_ex:
    print("Exception listing tables: {}".format(table_list_ex))
    sys.exit()


remodel.connection.pool.configure(db="fleet")

class Starship(Model):
    has_many = ("Crewmember",)

class Crewmember(Model):
    belongs_to = ("Starship",)

@app.route("/")
def ships():
    return flask.render_template("ships.html", ships=Starship.all())

@app.route("/ship/<ship_id>")
def ship(ship_id):
    ship = Starship.get(ship_id)
    crew = ship["crewmembers"].all()
    return flask.render_template("ship.html", ship=ship, crew=crew)

@app.route("/member/<member_id>")
def member(member_id):
    member = Crewmember.get(member_id)
    return flask.render_template("crew.html", member=member)

if __name__ == "__main__":
    remodel.utils.create_tables()
    remodel.utils.create_indexes()

    # Populate the DB
    voyager = Starship.create(name="Voyager", category="Intrepid", registry="NCC-74656")

    # Using .add method handles DB insertion in lieu of .create
    voyager["crewmembers"].add(
        Crewmember(name="Janeway", rank="Captain", species="Human"),
        Crewmember(name="Neelix", rank="Morale Officer", species="Talaxian"),
        Crewmember(name="Tuvok", rank="Lt Commander", species="Vulcan"))

    enterprise = Starship.create(name="Enterprise", category="Galaxy", registry="NCC-1701-D")
    enterprise["crewmembers"].add(
        Crewmember(name="Picard", rank="Captain", species="Human"),
        Crewmember(name="Data", rank="Lt Commander", species="Android"),
        Crewmember(name="Troi", rank="Counselor", species="Betazed"))

    defiant = Starship.create(name="Defiant", category="Defiant", registry="NX-74205")
    defiant["crewmembers"].add(
        Crewmember(name="Sisko", rank="Captain", species="Human"),
        Crewmember(name="Dax", rank="Lt Commander", species="Trill"),
        Crewmember(name="Kira", rank="Major", species="Bajoran"))

    # Query the DB
    voyager = Starship.get(name="Voyager")
    for human in voyager["crewmembers"].filter(species="Human"):
        print(human["name"])

    for person in Crewmember.filter(rank="Captain"):
        print(person["name"], "captain of", person["starship"]["name"])

    crewmember_reduction = Crewmember.table.group("species").ungroup() \
          .map(lambda item: [item["group"], item["reduction"].count()]) \
          .coerce_to("object").run()

    print(crewmember_reduction)

    app.run(host="localhost", port=8090, debug=True)