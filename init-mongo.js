db = db.getSiblingDB('f1_pipeline');

db.createCollection('car_data');
db.createCollection('laps');
db.createCollection('pit');
db.createCollection('position');
db.createCollection('weather');
db.createCollection('intervals');
db.createCollection('drivers');
db.createCollection('meetings');
db.createCollection('sessions');
db.createCollection('race_control');
db.createCollection('overtakes');
db.createCollection('stints');
db.createCollection('starting_grid');
db.createCollection('session_result');
db.createCollection('location');
db.createCollection('dead_letter');

print('MongoDB initialized successfully!');