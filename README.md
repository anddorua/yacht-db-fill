Database fake data filling utility
==================================

1. Run docker-compose file with yacht service.

2. Port 5432 expected to be open as database postgres port.

3. Run `node ./fill.js --clear -d` to fillout database with primary data.
 
4. Run `node ./fill.js -s <json data file>` to store generated data to file.

5. Run`node ./fill.js --clear -d -f <json data file>` to load data from file to database.
