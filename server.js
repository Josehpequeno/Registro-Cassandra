var assert = require('assert');
//”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var Cassandra = require('cassandra-driver');
//Replace Username and Password with your cluster settings
//var authProvider = new cassandra.auth.PlainTextAuthProvider('Username', 'Password');
//Replace PublicIP with the IP addresses of your clusters
var contactPoints = ['127.0.0.1'];//, 'PublicIP', 'PublicIP’'];
var client = new Cassandra.Client({ contactPoints: contactPoints, localDataCenter: 'datacenter1' });
//var client2 = new cassandra.Client({ contactPoints: contactPoints, localDataCenter: 'datacenter1', keyspace: 'grocery' });

//Ensure all queries are executed before exit
function execute(query, params) {
    return new Promise((resolve, reject) => {
        //console.log("Paramentro: ", params.length);
        if (params.length != 0) {
            //console.log("Paramentro: ", params);
            client.execute(query, params, (err, result) => {
                if (err) {
                    console.log(params);
                    return reject(err);
                }
                return resolve(result);
            });
        }
        else {
            //console.log("Paramentro vazio");
            client.execute(query, (err, result) => {
                if (err) {
                    return reject(err);
                }
                return resolve(result);
            });
        }
    });
}

//Execute the queries 
var query = `CREATE KEYSPACE IF NOT EXISTS registro WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor' : 1};`
var q1 = execute(query, []).then(result => {
    console.log('Done');
}).catch(err => {
    console.log(err);
});
query = `CREATE TABLE IF NOT EXISTS registro.casamento (id VARCHAR, name1 TEXT, name2 TEXT, data date, PRIMARY KEY (id));`
q1 = execute(query, []).then(result => {
    console.log('Done');
}).catch(err => {
    console.log(err);
});
let data = new Date("Jan 29, 2023");
const meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"];
let dia = data.getDate();
if (dia < 10) {
    dia = String("0" + dia);
}
data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));
console.log(data);
data.split("-");
query = `INSERT INTO registro.casamento (id,name1, name2, data) VALUES ('a1','Joseh','Marcus',???);`
q1 = execute(query, [data[0],data[1],data[2]]).then(result => {
    console.log('Done');
}).catch(err => {
    console.log(err);
});
data = new Date("Apr 2, 2016");
dia = data.getDate();
if (dia < 10) {
    dia = String("0" + dia);
}
data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));
console.log(data);
data.split("-");
query = `INSERT INTO registro.casamento (id, name1, name2, data) VALUES ('b2','John','Esqueci',???);`
q1 = execute(query, [data[0],data[1],data[2]]).then(result => {
    console.log('Done');
}).catch(err => {
    console.log(err);
});
/*
query = `SELECT name, price_p_item FROM registro.casamento WHERE name=? ALLOW FILTERING`;
q1 = execute(query, ['oranges']).then(result => {
    console.log('The cost per orange is', Number(result.rows[0].price_p_item));
}).catch(err => {
    console.log(err)
});*/
query = `SELECT * FROM registro.casamento;`;
q1 = execute(query, []).then(result => {
    console.log("#######################################################");
    for (const row of result.rows) {
        console.log(
            "Linha de Casamentos: %s | %s | %s | %s ",
            row.id,
            row.name1,
            row.name2,
            row.data
        );
    }
    console.log("#######################################################");
    //client.shutdown();
}).catch(err => {
    if (err != undefined) {
        console.log(err);
    }
    //client.shutdown();
});
/*require('dotenv').config();
const cassandra = require('cassandra-driver');
const authProvider = new cassandra.auth.PlainTextAuthProvider(
    process.env.CASSANDRA_USER,
    process.env.CASSANDRA_PASS,
);
const client = new cassandra.Client({
    contactPoints: ['ip-address, ip-address, ip-address'],
    localDataCenter: 'datacentername',
    authProvider,
    keyspace: 'keyspacename',
});
//client.execute(query, parameters, options)
const findAllByListing = async (parameters) => {
    const query = 'SELECT * FROM reviews_by_listing WHERE listing_id   = ?;';
    const result = await client.execute(query, parameters, { prepare: true });
    return result;
};*/