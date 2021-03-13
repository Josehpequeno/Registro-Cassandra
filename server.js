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
        /*console.log("{");
        console.log("Paramentro: ", params);
        console.log("Paramentro: ", params.length);
        console.log("Query: ", query);
        console.log("}");
        */
        client.execute(query, params,/* { prepare: true },*/(err, result) => {
            if (err) {
                console.log(params);
                return reject(err);
            }
            return resolve(result);
        });
    });
}

function Registros() {
    query = `SELECT * FROM registro.casamento;`;
    q1 = execute(query, []).then(result => {
        console.log();
        console.log("########################################");
        for (const row of result.rows) {
            console.log(
                "Linha: %s | %s | %s | %s ",
                row.id,
                row.name1,
                row.name2,
                row.data
            );
        }
        console.log("########################################");
        //client.shutdown();
    }).catch(err => {
        if (err != undefined) {
            console.log(err);
        }
        client.shutdown();
    });
}

//Execute the queries 
var query = `CREATE KEYSPACE IF NOT EXISTS registro WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor' : 1};`
var q1 = execute(query, []).then(result => {
    console.log('Done Keyspace');
}).catch(err => {
    console.log(err);
});
query = `CREATE TABLE IF NOT EXISTS registro.casamento (id VARCHAR, name1 TEXT, name2 TEXT, data DATE, PRIMARY KEY (id));`
q1 = execute(query, []).then(result => {
    console.log('Done Table');
}).catch(err => {
    console.log(err);
});
query = `DELETE FROM registro.casamento WHERE id IN ('a1', 'b2','c3','d4');`
q1 = execute(query, []).then(result => {
    console.log('Done Delete');
    Registros();
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
//console.log(data);
data = data;
query = `INSERT INTO registro.casamento (id,name1, name2, data) VALUES ('a1','Joseh','Marcus', '${data}');`
q1 = execute(query, []).then(result => {
    console.log('Done Insert-0');
}).catch(err => {
    console.log(err);
});
data = new Date("Jan 24, 2017");
dia = data.getDate();
if (dia < 10) {
    dia = String("0" + dia);
}
data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));
//console.log(data);
query = `INSERT INTO registro.casamento (id, name1, name2, data) VALUES ('b2','Jhon','Camila','${data}');`
q1 = execute(query, []).then(result => {
    console.log('Done Insert-1');
}).catch(err => {
    console.log(err);
});
query = `INSERT INTO registro.casamento (id, name1, name2) VALUES ('c3','Hiago','Taina');`
q1 = execute(query, []).then(result => {
    console.log('Done Insert-2');
    Registros();
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
data = new Date("Sept 18, 2022");
dia = data.getDate();
if (dia < 10) {
    dia = String("0" + dia);
}
data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));

query = `INSERT INTO registro.casamento (id, name1, name2,data) VALUES ('c3','Hiago','Taina','${data}');`
q1 = execute(query, []).then(result => {
    console.log('Done Insert-3');
    Registros();
}).catch(err => {
    console.log(err);
});
query = `UPDATE registro.casamento SET data = '${'2022-03-25'}' WHERE id = 'c3';`
q1 = execute(query, []).then(result => {
    console.log('Done Update');
    Registros();
}).catch(err => {
    console.log(err);
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