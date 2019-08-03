# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import desc

import numpy as np
import datetime as dt
import pandas as pd

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii1.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station




# Create our session (link) from Python to the DB

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################


@app.route("/")
def welcome():
    """List all the available routes"""

    return(
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/ends<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """ Retrieve the date and precipitation scores from the last year"""

    # Create a session
    session = Session(engine)

    # Latest Date
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    print("Latest Date: ",latest_date)

    l_date = dt.datetime.strptime(latest_date, '%Y-%m-%d')
    one_year_ago = l_date - dt.timedelta(days=365)
    print (one_year_ago)

    # Perform a query to retrieve the data and precipitation scores
    # Save the query results as a Pandas DataFrame and set the index to the date column

    prec = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date >= one_year_ago).order_by(desc(Measurement.date)).all()

    # Convert list of tuples into normal list
    all_prec = list(np.ravel(prec))

    return jsonify(all_prec)

@app.route("/api/v1.0/stations")
def stations():
    # Create a session
    session = Session(engine)

    stationsN = session.query(Measurement.station, func.count(Measurement.station).label('N_rows')).\
          group_by(Measurement.station).order_by(desc('N_rows')).all()

    # Convert list of tuples into normal list
    stations_observations = list(np.ravel(stationsN))

    return jsonify(stations_observations)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create a session
    session = Session(engine)

    # Latest Date
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]
    print("Latest Date: ",latest_date)

    l_date = dt.datetime.strptime(latest_date, '%Y-%m-%d')
    one_year_ago = l_date - dt.timedelta(days=365)
    print (one_year_ago)

    # Perform a query to retrieve the data and precipitation scores
    # Save the query results as a Pandas DataFrame and set the index to the date column

    tobs_q = session.query(Measurement.date, Measurement.tobs).\
        filter(Measurement.date >= one_year_ago).order_by(desc(Measurement.date)).all()

    # Convert list of tuples into normal list
    last_year_tobs = list(np.ravel(tobs_q))

    return jsonify(last_year_tobs)

@app.route("/api/v1.0/start")
def start():

    # Create a session
    session = Session(engine)

    # Latest Date
    latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()[0]

    start_d = "2017-01-01"
    start = dt.datetime.strptime(start_d, '%Y-%m-%d')
    end = dt.datetime.strptime(latest_date, '%Y-%m-%d')
    step = dt.timedelta(days=1)

    date_range = []
    while start <= end:
        date_range.append(start.date())
        start += step
    
    data1 = []
    for d in date_range:
        data1.append(list(np.ravel(session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date == d).all())))

    return jsonify(data1)

@app.route("/api/v1.0/ends")
def ends():

    # Create a session
    session = Session(engine)
    
    start_date = "2017-01-01"
    end_date = "2017-01-07"

    ends1 = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
        filter(Measurement.date >= start_date).filter(Measurement.date <= end_date).all()

    ends2 = list(np.ravel(ends1))

    return jsonify(ends2)

if __name__ == '__main__':
    app.run(debug=True)
