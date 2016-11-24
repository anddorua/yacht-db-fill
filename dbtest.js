/**
 * Created by andriy on 22.11.16.
 */
'use strict';

const faker = require("faker");
const pg = require('pg');
const fs = require('fs');

var pgUser = "postgres";
var pgPassword = "1q2w3e4r";
var pgHost = "localhost";
var pgDatabase = "postgres";

const conString = "postgres://" + pgUser + ":" + pgPassword + "@" + pgHost + "/" + pgDatabase;
var entities = {
    "my_yacht.user": { "amount": 2, "generator": genUser, "key": "id" },
    "my_yacht.devices": { "amount": 2, "generator": genDevices, "key": "id", "foreign": [ { fTable: "my_yacht.user", keys: { my: "user_id", foreign: "id" } } ] },
    "my_yacht.yacht": { "amount": 1000, "generator": genYacht, "key": "id" },
    "my_yacht.booking": { "amount": 10, "generator": genBooking, "key": "id", "foreign": [ { fTable: "my_yacht.user", keys: { my: "user_id", foreign: "id" } }, { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  },
    "my_yacht.file": { "amount": 10, "generator": genFile, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  },
    "my_yacht.packages": { "amount": 100, "generator": genPackages, "key": "id", "foreign": [ { fTable: "my_yacht.yacht", keys: { my: "y_id", foreign: "id" } } ]  }
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
    generateEntities();
    saveEntities(client);
    saveChain.then(function(res){
        console.log("================ all saved =================");
        printEntities();
    }, function(err){
        console.log("Error occured");
    })
        .then(function (res) {
            done();
        }, function (err) {
            done();
        });
});

return 0;

function* genUser(amount) {
    for (let i = 0; i < amount; i++) {
        yield {
            id: null,
            firstname: faker.name.firstName().substr(0, 80),
            lastname: faker.name.lastName().substr(0, 80),
            email: faker.internet.email().substr(0, 255),
            mobile: faker.phone.phoneNumber().substr(0, 16),
            password: "password",
            role: faker.random.arrayElement(["manager", "user_role"]),
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
    var src = require('./yacht.json');
    for (let i = 0; i < amount && i < src.length; i++) {
        yield src[i];
    }
    return null;
}

function* genPackages(amount) {
    var src = require('./packages.json');
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
        console.log("Dates generated: ", startDate, endDate);
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
        console.log("Dates generated: ", startDate, endDate);
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
            console.log(eItem);
        });
    }
}

// entities save and binding
function saveEntities(pgClient) {
    for (let entity in entities) {
        console.log("Going to save " + entity);
        if (!saveEntity(entity, pgClient)) {
            console.error("Entity " + fKey.fTable + ": closure occured.");
        } else {
            console.log(entity + " chained");
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
    console.log("Make Chain to save entity " + entity);
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
                    console.log("SQL:", query["state"]);
                    console.log("PARAMS:", query["params"]);
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
                console.log("Entity " + _entity + " saved:", _eItem);
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
    console.log("sql state:", state);
    console.log("sql params: ", params);
    callback(false, { rows: [ ++genNumbersCurrent ] });
}