/**
 * Created by andriy on 22.11.16.
 */
'use strict';

const faker = require("faker");
const pg = require('pg');
const fs = require('fs');
const moment = require('moment');
const argv = require('minimist')(process.argv);

if (Object.keys(argv).length == 1) {
    console.log("Usage: node " + argv._[0] + " [-clear] [-v] [-d] [-s <filename_to_save>] | [-f <filename_to_load>] ]");
    console.log("    where:");
    console.log("    -f <filename_to_load> - load json data from file, do not generate it (note: use ./ prefix for local files)");
    console.log("    -s <filename_to_save> - save generated data to file");
    console.log("    -d - put data to database");
    console.log("    -v - verbose mode, print data and actions");
    console.log("    --clear - clear tables before filling");
    console.log("    It uses standard environment postgres variables to access to database: PGUSER, PGPASSWORD, PGHOST, PGDATABASE");
    return 0;
}


var pgUser = process.env.PGUSER || "postgres";
var pgPassword = process.env.PGPASSWORD || "1q2w3e4r";
var pgHost = process.env.PGHOST || "localhost";
var pgDatabase = process.env.PGDATABASE || "postgres";

const conString = "postgres://" + pgUser + ":" + pgPassword + "@" + pgHost + "/" + pgDatabase;
var entities = {
    "my_yacht.user": { "amount": 5, "generator": genUser, "key": "id" },
    "my_yacht.devices": { "amount": 10, "generator": genDevices, "key": "id", "foreign": [ { fTable: "my_yacht.user", keys: { my: "user_id", foreign: "id" } } ] },
    "my_yacht.yacht": { "amount": 1000, "generator": genYacht, "key": "id" },
    "my_yacht.yacht_description": { "amount": 1000, "generator": genYachtDescription, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "yacht_id", foreign: "id" } } ] },
    /* "my_yacht.booking": { "amount": 10, "generator": genBooking, "key": "id", "foreign": [ { fTable: "my_yacht.user", keys: { my: "user_id", foreign: "id" } }, { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  }, */
    "my_yacht.file": { "amount": 10, "generator": genFile, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  },
    "my_yacht.packages": { "amount": 100, "generator": genPackages, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  },
    "my_yacht.extras": { "amount": 100, "generator": genExtras, "key": "id"  } ,
    "my_yacht.createbooking": { "amount": 10, "generator": genBooking, "key": "id", "saver": "bookingSender", "foreign": [
        { fTable: "my_yacht.yacht", "fixer": "bookingYachtKeyFiller" },
        { fTable: "my_yacht.packages", "fixer": "bookingPackagesKeyFiller" },
        { fTable: "my_yacht.extras", "fixer": "bookingExtrasKeyFiller" }
    ]   }
};

Date.prototype.addDays = function(days)
{
    var dat = new Date(this.valueOf());
    dat.setDate(dat.getDate() + days);
    return dat;
};

var genNumbersCurrent = 0;

var saveChain = Promise.resolve(true);

function conOut() {
    if (argv.v) {
        console.log(arguments);
    }
}

// load / generate data
if (argv.f) {
    entities = require(argv.f)
} else {
    generateEntities();
}
if (argv.s) {
    fs.writeFile(argv.s, JSON.stringify(entities), function(err){
        if (err) {
            console.error("Error to write file " + argv.s + ": ", err);
            process.exit(1);
        } else {
            console.log("Data written to " + argv.s);
        }
    });
}

if (argv.d) {
    pg.connect(conString, function (err, client, done) {
        if (err) {
            return console.error('error fetching client from pool', err)
        }
        // save to file if required
        // save data to database
        if (argv.d) {
            if (argv.clear) {
                conOut("Going to clear tables before fill.");
                saveChain = clearDatabase(saveChain, client);
            } else {
                if (argv.v) {
                    conOut("Database not cleared.");
                }
            }
            saveEntities(client, saveChain);
            saveChain.then(function(res){
                conOut("================ all saved =================");
            }, function(err){
                console.error("Error occured while database filling", err);
            })
                .then(function (res) {
                    done();
                    client.end(function (err) {
                        if (err) throw err;
                    });
                    process.exit();
                }, function (err) {
                    done();
                    client.end(function (err) {
                        if (err) throw err;
                    });
                    process.exit();
                });
        } else {
            console.log("Skip filling database");
            done();
            client.end(function (err) {
                if (err) throw err;
            });
            process.exit();
        }
    });
}

console.log("All done.");
//return 0;

function* genUser(amount) {
    var src = require('./predefined/user.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    for (let i = 0; i < (amount - src.length); i++) {
        yield createRandomUser();
    }
}

function createRandomUser() {
    return {
        id: faker.random.number({ min: 1000, max: 2000, precision: 1 }),
        firstname: faker.name.firstName().substr(0, 80),
        lastname: faker.name.lastName().substr(0, 80),
        email: faker.internet.email().substr(0, 255),
        mobile: faker.phone.phoneNumber().substr(0, 16),
        password: "password",
        role: faker.random.arrayElement(["user_role"]),
        discount: faker.random.arrayElement([faker.random.number({ min: 0, max: 0.1, precision: 0.01 }), null])
    };
}

function* genDevices(amount) {
    let userIds = entities["my_yacht.user"].items.map(function(item){ return item.id; });
    for (let i = 0; i < amount; i++) {
        yield {
            id: null,
            user_id: faker.random.arrayElement(userIds),
            platform: faker.internet.userAgent().substr(0, 45),
            device_id: faker.internet.userAgent().substr(0, 45)
        };
    }
}

function* genYacht(amount) {
    var src = require('./predefined/yacht.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    return null;
}

function* genYachtDescription(amount) {
    var src = require('./predefined/yacht_description.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    return null;
}

function* genPackages(amount) {
    var src = require('./predefined/packages.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    return null;
}

function* genExtras(amount) {
    var src = require('./predefined/extras.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    return null;
}

function* genBooking(amount) {
    let users = entities["my_yacht.user"]["items"];
    let yachts = entities["my_yacht.yacht"]["items"];
    let now = moment();
    // making past orders
    var current = now.minute(0).second(0).millisecond(0).subtract(1, 'months').clone();
    for (let i = 0; i < amount; i++) {
        yield createBooking(current, yachts, users, amount);
    }

    now = moment();
    current = now.minute(0).second(0).millisecond(0).subtract(2, 'days').clone();
    for (let i = 0; i < amount; i++) {
        yield createBooking(current, yachts, users, amount);
    }
}

function createBooking(current, yachts, users, amount) {
    let yacht = faker.random.arrayElement(yachts);
    let booking = genBookingMain(users, yachts, current);
    let bookingStart = new moment(booking.start_date);
    let bookingEnd = new moment(booking.end_date);
    let bookingHours = (bookingEnd.unix() - bookingStart.unix()) / 3600;

    booking.additionals = [];
    booking.additionals.push(genCharterAdditional(yacht));

    let packAmount = faker.random.number({ min: 0, max: 3 });
    for (let j = 0; j < packAmount; j++) {
        booking.additionals.push(genNonCharterAdditional(booking.guests));
    }

    let extraAmount = faker.random.number({ min: 0, max: 3 });
    for (let j = 0; j < extraAmount; j++) {
        booking.additionals.push(genExtra(bookingHours));
    }
    return booking;
}

function genExtra(hours) {
    let extra = faker.random.arrayElement(entities["my_yacht.extras"]["items"]);
    let additional = {};
    additional.extrasId = extra.id;
    additional.packageId = null;
    var amount = 0;
    switch(extra.unit) {
        case "Per Trip / Hour":
            amount = extra.min_charge;
            break;
        case "Per hour":
            amount = hours;
            break;
        case "Per event":
            amount = 1;
            break;
        default:
            amount = 1;
    }
    additional.amount = amount;
    additional.money = extra.price * amount;
    return additional;
}

function genNonCharterAdditional(guests) {
    let nonCharter = entities["my_yacht.packages"]["items"].filter(function(pack) { return pack.y_id === null; } );
    //console.log(nonCharter);
    let pack = faker.random.arrayElement(nonCharter);
    let additional = {};
    additional.extrasId = null;
    additional.packageId = pack.id;
    additional.amount = Math.max(pack.min_charge, guests);
    additional.money = pack.price * additional.amount;
    return additional;
}

function genCharterAdditional(yacht) {
    // yacht package
    let yRentPack = getBookingYachtPackage(yacht);
    let additional = {};
    additional.extrasId = null;
    additional.packageId = yRentPack.id;
    additional.amount = faker.random.number({ min: yRentPack.min_charge, max: 28 });
    additional.money = yRentPack.price * additional.amount;
    return additional;
}

function getBookingYachtPackage(yacht) {
    let packId = entities["my_yacht.packages"]["items"].findIndex(function(p){ return p.y_id == yacht.id; });
    if (packId == -1) {
        console.error("Can`t find package for yacht ", yacht);
        process.exit(1);
    }
    return entities["my_yacht.packages"]["items"][packId];
}

function genBookingMain(users, yachts, current) {
    let user = faker.random.number({ min: 1, max: 2 }) > 1 ? faker.random.arrayElement(users) : createRandomUser();
    let yacht = faker.random.arrayElement(yachts);
    let booking = {};
    booking.email = user.email;
    current.add(faker.random.number({ min: 1, max: 24 }), 'h'); // seed start time
    //booking.start_date = current.format('YYYY MM DD HH:mm:ssZZ');
    booking.start_date = current.format();
    booking.end_date = current.add(faker.random.number({ min: 4, max: 28 }), 'h').format();
    booking.guests = faker.random.number({ min: 10, max: yacht.max_guests });
    booking.firstname = user.firstname;
    booking.lastname = user.lastname;
    booking.payment_type = "Method 1";
    booking.phone = user.mobile;
    booking.user_id = null;
    booking.y_id = yacht.id;
    return booking;
}

var fixers = {};

fixers.bookingYachtKeyFiller = function(item, oldKey, newKey) {
    if (item.y_id == oldKey) {
        item.y_id = newKey;
    }
};

fixers.bookingPackagesKeyFiller = function (item, oldKey, newKey) {
    item.additionals.forEach(function(additional){
        if (additional.packageId == oldKey) {
            additional.packageId = newKey;
        }
    });
};

fixers.bookingExtrasKeyFiller = function (item, oldKey, newKey) {
    item.additionals.forEach(function(additional){
        if (additional.extrasId == oldKey) {
            additional.extrasId = newKey;
        }
    });
};

var savers = {};
savers.bookingSender = function (pgClient, entityName, eItem, resolve, reject) {
    conOut("fake save for " + entityName + " " + JSON.stringify(eItem));
    pgClient.query("select * from my_yacht.createbooking($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", [
        eItem.email,
        eItem.start_date,
        eItem.end_date,
        eItem.guests,
        eItem.firstname,
        eItem.lastname,
        eItem.payment_type,
        eItem.phone,
        eItem.user_id,
        eItem.y_id,
        JSON.stringify(eItem.additionals)
    ], function (err, result) {
        if (err) {
            reject(err);
        } else {
            resolve(result.rows);
        }
    });
};


function* genInvoice(amount) {
    for (let i = 0; i < amount; i++) {
        var now = new Date();
        var startDate = new Date(now.getTime() - Math.floor(Math.random() * 100) *86400 * 1000);
        var endDate = new Date(startDate.getTime() + Math.floor(Math.random() * 200) *86400 * 1000);
        endDate.addDays( Math.floor(Math.random() * 200));
        yield {
            id: null,
            y_id: null,
            start_date: startDate,
            end_date: endDate,
            user_id:  null,
            payment: faker.finance.amount(1000, 5000, 2), //faker.random.arrayElement([faker.finance.amount(1000, 5000, 2), null]),
            status: faker.random.number({ min: 1, max: 5 })
        };
    }
}

function* genFile(amount) {
    let yIds = entities["my_yacht.yacht"].items.map(function(item){ return item.id; });
    for (let i = 0; i < amount; i++) {
        yield {
            id: null,
            type:  faker.random.arrayElement(["image", "drawing", "description"]),
            url: faker.image.transport(),
            y_id: faker.random.arrayElement(yIds)
        };
    }
}

// entities generation
function generateEntities() {
    for (let entityItem in entities) {
        var gen = entities[entityItem].generator(entities[entityItem].amount);
        entities[entityItem].items = [];
        for (let entity of gen) {
            entities[entityItem].items.push(entity);
        }
    }
}

// entities print
function printEntities() {
    for (let entityItem in entities) {
        entities[entityItem].items.forEach(function (eItem) {
            if (argv.v) {
                console.log("Entity item: ", eItem);
            }
        });
    }
}

// entities save and binding
function saveEntities(pgClient, chain) {
    for (let entity in entities) {
        conOut("Going to save " + entity);
        if (!saveEntity(entity, pgClient, chain)) {
            console.error("Entity " + fKey.fTable + ": closure occured.");
        } else {
            conOut(entity + " chained");
        }
    }
}

/**
 * Fixes foreign keys when item saves and changes it
 * @param entityName name of the entity which primary key was changed
 * @param oldFk old primary key
 * @param newFk new primary key
 */
function fixForeignKeys(entityName, oldFk, newFk) {
    for (let eName in entities) {
        conOut("Find fk in " + eName);
        var entityMeta = entities[eName];
        if (!entityMeta.foreign) {
            continue;
        }
        let fk = entityMeta.foreign.find(function (fkItem) {
            return fkItem.fTable == entityName;
        });
        if (!fk) {
            continue;
        }
        conOut("Fix foreign key for entities " + eName, fk);
        entityMeta.items.forEach(function(item){
            //define primary key if not present
            if (Object.keys(item).indexOf(entityMeta.key) == -1) {
                item[entityMeta.key] = null;
            }
            if (fk.keys) {
                // have explicit defined foreign keys
                if (item[fk.keys.my] === oldFk) {
                    conOut("Entity " + eName + " with pk=" + item[entityMeta.key] + " fk[" + fk.keys.my + "]=="
                        + (item[fk.keys.my] === null ? 'null' : item[fk.keys.my]) + " => " + newFk);
                    item[fk.keys.my] = newFk;
                }
            } else if (fk.fixer) {
                fixers[fk.fixer](item, oldFk, newFk);
            } else {
                console.error("No rule for fixing foreign keys in meta " + eName + " for table " + fk.fTable);
            }
        });
    }
}

function saveEntity(entity, pgClient, chain) {
    if (entities[entity].done) {
        return true;
    }
    if (entities[entity].inProgress) {
        return false;
    }
    entities[entity].inProgress = true;
    if (entities[entity].foreign) {
        entities[entity].foreign.forEach(function (fKey) {
            if (!saveEntity(fKey.fTable, pgClient, chain)) {
                console.error("Entity " + fKey.fTable + ": closure occured.");
                return false;
            }
        })
    }
    if (argv.v) {
        console.log("Make Chain to save entity " + entity);
    }
    saveChain = saveChain.then(function(res){
        return Promise.all(saveItems(entity, pgClient));
    }, function(err){
        console.error("Error during save " + entity, err);
        return err;
    });
    entities[entity].inProgress = false;
    entities[entity].done = true;
    return true;
}

// saves items to table and stores primary keys
function saveItems(entity, pgClient) {
    var promises = [];
    //var itemsChain = Promise.resolve(true);
    entities[entity].items.forEach(function (eItem) {
        //save item
        promises.push( (function (_entityName, _eItem) {
            let savePromise = new Promise(function(resolve, reject){
                if (_entityName == "my_yacht.createbooking") {
                    var a = true;
                }
                if (entities[_entityName].saver) {
                    conOut("call saver for:" + JSON.stringify(_eItem));
                    savers[entities[_entityName].saver](pgClient, _entityName, _eItem, resolve, reject);
                } else {
                    let query = constructInsertQuery(_entityName, _eItem);
                    conOut("SQL:", query["state"]);
                    conOut("PARAMS:", query["params"]);
                    pgClient.query(query["state"], query["params"], function (err, result) {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(result.rows[0][entities[_entityName].key]);
                        }
                    });
                }
            });
            savePromise.then(function(key){
                conOut("Entity " + _entityName + " saved:", _eItem);
                let oldKey = _eItem[entities[_entityName].key];
                conOut("Old key for entity :" + _entityName, oldKey);
                conOut("New key for entity :" + _entityName, key);
                if (oldKey !== null) {
                    conOut("Going to fix foreign keys pointing to " + _entityName + " with pk=", oldKey);
                    fixForeignKeys(_entityName, oldKey, key);
                }
                _eItem[entities[_entityName].key] = key;
            }, function(err){
                console.error('error happened during query ', err);
            });
            return savePromise;
        }) (entity, eItem) );
    });
    return promises;
}

function constructInsertQuery(entity, item) {
    let fields = Object.keys(item);
    fields.splice(fields.indexOf(entities[entity].key), 1);
    var result = {};
    let paramList = [];
    for(let i = 1; i <= fields.length; i++) {
        paramList.push("$" + i);
    }
    result["state"] = "INSERT INTO " + entity + " (" + fields.join(",") + ") VALUES (" + paramList.join(",") + ") RETURNING " + entities[entity].key;
    result["params"] = [];
    fields.forEach(function (field) {
        var itemValue;
        if (Array.isArray(item[field])) {
            itemValue = item[field].join('');
        } else {
            itemValue = item[field];
        }
        result["params"].push(itemValue);
    });
    return result;
}

function fakeQuery(state, params, callback) {
    conOut("sql state:", state);
    conOut("sql params: ", params);
    callback(false, { rows: [ ++genNumbersCurrent ] });
}

function clearDatabase(chain, pgClient) {
    var queries = [
        "delete from my_yacht.download",
        "delete from my_yacht.payment",
        "delete from my_yacht.invoice",
        "delete from my_yacht.booking",
        "delete from my_yacht.additional",
        "delete from my_yacht.file",
        "delete from my_yacht.packages",
        "delete from my_yacht.extras",
        "delete from my_yacht.devices",
        "delete from my_yacht.user",
        "delete from my_yacht.yacht",
    ];
    queries.forEach(function(query) {
        chain = chain.then(function(res){
            return new Promise(function (resolve, reject) {
                conOut("chain query :", query);
                pgClient.query(query, [], function (err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        conOut("sql state:", query);
                        resolve(true);
                    }
                });
            })
        }, function (err) {
            console.error("Error during database clear:", err);
            return err;
        })
    });
    return chain;
}