/**
 * Created by andriy on 22.11.16.
 */
'use strict';

const faker = require("faker");
const pg = require('pg');
const fs = require('fs');
const argv = require('minimist')(process.argv);

if (Object.keys(argv).length == 1) {
    console.log("Usage: node " + argv._[0] + " [-clear] [-v] [-d] [-s <filename_to_save>] | [-f <filename_to_load>] ]");
    console.log("    where:");
    console.log("    -f <filename_to_load> - load json data from file, do not generate it (note: use ./ prefix for local files)");
    console.log("    -s <filename_to_save> - save generated data to file");
    console.log("    -d - put data to database");
    console.log("    -v - verbose mode, print data and actions");
    console.log("    -clear - clear tables before filling");
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
    /* "my_yacht.booking": { "amount": 10, "generator": genBooking, "key": "id", "foreign": [ { fTable: "my_yacht.user", keys: { my: "user_id", foreign: "id" } }, { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  }, */
    "my_yacht.file": { "amount": 10, "generator": genFile, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  },
    "my_yacht.packages": { "amount": 100, "generator": genPackages, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  },
    "my_yacht.extras": { "amount": 100, "generator": genExtras, "key": "id"  }
};

Date.prototype.addDays = function(days)
{
    var dat = new Date(this.valueOf());
    dat.setDate(dat.getDate() + days);
    return dat;
};

var genNumbersCurrent = 0;

var saveChain = Promise.resolve(true);

pg.connect(conString, function (err, client, done) {
    if (err) {
        return console.error('error fetching client from pool', err)
    }
    // load / generate data
    if (argv.f) {
        entities = require(argv.f)
    } else {
        generateEntities();
    }
    // save to file if required
    if (argv.s) {
        fs.writeFile(argv.s, JSON.stringify(entities), function(err){
            if (err) {
                console.error("Error to write file " + argv.s + ": ", err);
            } else {
                console.log("Data written to " + argv.s);
            }
        });
    }
    // save data to database
    if (argv.d) {
        if (argv.clear) {
            if (argv.v) {
                console.log("Going to clear tables before fill.");
            }
            saveChain = clearDatabase(saveChain, client);
        } else {
            if (argv.v) {
                console.log("Database not cleared.");
            }
        }
        saveEntities(client);
        saveChain.then(function(res){
            if (argv.v) {
                console.log("================ all saved =================");
                printEntities();
            }
        }, function(err){
            console.error("Error occured while database filling", err);
        })
            .then(function (res) {
                done();
            }, function (err) {
                done();
            });
    } else {
        console.log("Skip filling database");
    }
});

console.log("All done.");
return 0;

function* genUser(amount) {
    var src = require('./predefined/user.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    for (let i = 0; i < (amount - src.length); i++) {
        yield {
            id: null,
            firstname: faker.name.firstName().substr(0, 80),
            lastname: faker.name.lastName().substr(0, 80),
            email: faker.internet.email().substr(0, 255),
            mobile: faker.phone.phoneNumber().substr(0, 16),
            password: "password",
            role: faker.random.arrayElement(["user_role"]),
            discount: faker.random.arrayElement([faker.random.number({ min: 0, max: 0.1, precision: 0.01 }), null])
        };
    }
}

function* genDevices(amount) {
    for (let i = 0; i < amount; i++) {
        yield {
            id: null,
            user_id: null,
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
            status: faker.random.number({ min: 1, max: 5 }),
            payment_type: faker.random.arrayElement(["card", "paypal"])
        };
    }
}

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
    for (let i = 0; i < amount; i++) {
        yield {
            id: null,
            type:  faker.random.arrayElement(["image", "drawing", "description"]),
            url: faker.image.transport(),
            y_id: null
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
function saveEntities(pgClient) {
    for (let entity in entities) {
        if (argv.v) {
            console.log("Going to save " + entity);
        }
        if (!saveEntity(entity, pgClient)) {
            console.error("Entity " + fKey.fTable + ": closure occured.");
        } else {
            if (argv.v) {
                console.log(entity + " chained");
            }
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
        var entityMeta = entities[eName];
        if (!entityMeta.foreign) {
            return;
        }
        let fk = entityMeta.foreign.find(function (fkItem) {
            return fkItem.fTable == entityName;
        });
        if (!fk) {
            return;
        }
        entityMeta.items.forEach(function(item){
            if (item[fk.keys.my] === oldFk) {
                item[fk.keys.my] = newFk;
            }
        });
    }
}

function saveEntity(entity, pgClient) {
    if (entities[entity].done) {
        return true;
    }
    if (entities[entity].inProgress) {
        return false;
    }
    entities[entity].inProgress = true;
    if (entities[entity].foreign) {
        entities[entity].foreign.forEach(function (fKey) {
            if (!saveEntity(fKey.fTable)) {
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
    entities[entity].items.forEach(function (eItem) {
        //collect foreign keys
        if (entities[entity].foreign) {
            entities[entity].foreign.forEach(function (fk) {
                let forCount = entities[fk.fTable].items.length;
                let foreignItem = entities[fk.fTable].items[ Math.floor(Math.random() * forCount) ];
                eItem[fk.keys.my] = foreignItem[fk.keys.foreign];
            })
        }
        //save item
        promises.push( (function (_entity, _eItem) {
            let savePromise = new Promise(function(resolve, reject){
                let query = constructInsertQuery(_entity, _eItem);
                if (argv.v) {
                    console.log("SQL:", query["state"]);
                    console.log("PARAMS:", query["params"]);
                }
                pgClient.query(query["state"], query["params"], function (err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(result.rows[0][entities[_entity].key]);
                    }
                });
            });
            savePromise.then(function(key){
                let oldKey = _eItem[entities[_entity].key];
                if (oldKey !== null) {
                    fixForeignKeys(_entity, oldKey, key);
                }
                _eItem[entities[_entity].key] = key;
                if (argv.v) {
                    console.log("Entity " + _entity + " saved:", _eItem);
                }
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
        result["params"].push(item[field]);
    });
    return result;
}

function fakeQuery(state, params, callback) {
    if (argv.v) {
        console.log("sql state:", state);
        console.log("sql params: ", params);
    }
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
                if (argv.v) {
                    console.log("chain query :", query);
                }
                pgClient.query(query, [], function (err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        if (argv.v) {
                            console.log("sql state:", query);
                        }
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