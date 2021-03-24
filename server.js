var assert = require('assert');
//”cassandra-driver” is in the node_modules folder. Redirect if necessary.
var Cassandra = require('cassandra-driver');
const { resolve } = require('path');
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

function Registros(q) {
    return new Promise((resolve, reject) => {
        query = `SELECT * FROM registro.casamento;`;
        q1 = execute(query, []).then(result => {
            console.log();
            //console.log("Query: {");
            for (let i = 0; i < q.length; i++) {
                console.log(q[i]);
                console.log("");
            }
            console.log(query);
            //console.log("}");

            console.log("");
            for (let row of result.rows) {
                if (row.name1 == null) {
                    row.name1 = "";
                }
                if (row.name2 == null) {
                    row.name2 = "";
                }
                if (row.data == null) {
                    row.data = "";
                }
                console.log(
                    "Linha: %s | %s | %s | %s ",
                    row.id,
                    row.name1,
                    row.name2,
                    row.data
                );
            }
            console.log("");
            resolve()
            //client.shutdown();
        }).catch(err => {
            if (err != undefined) {
                console.log(err);
                reject(err);
            }
            client.shutdown();
        });
    });
}

//Execute the queries
let promisesInicial = [];
var query1 = `CREATE KEYSPACE IF NOT EXISTS registro WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor' : 1};`
var q1 = execute(query1, []);
promisesInicial.push(q1);
var query2 = `CREATE TABLE IF NOT EXISTS registro.casamento (id VARCHAR, name1 TEXT, name2 TEXT, data DATE, PRIMARY KEY (id));`
var q2 = execute(query2, []);
promisesInicial.push(q2);
var query3 = `DELETE FROM registro.casamento WHERE id = 'a1';`
var q3 = execute(query3, []);
var query4 = `DELETE FROM registro.casamento WHERE id = 'b2';`
var q4 = execute(query4, []);
var query5 = `DELETE FROM registro.casamento WHERE id = 'c3';`
var q5 = execute(query5, []);

promisesInicial.push(q3);
promisesInicial.push(q4);
promisesInicial.push(q5);
const promisesAll = Promise.all(promisesInicial);
promisesAll.then((result) => {
    Registros([query1, query2, query3, query4, query5]).then(result => {


        console.log("Feito querys iniciais\n");
        let promises = [];

        let data = new Date("Jan 29, 2023");
        const meses = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"];
        let dia = data.getDate();
        if (dia < 10) {
            dia = String("0" + dia);
        }
        data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));
        //console.log(data);
        data = data;
        query1 = `INSERT INTO registro.casamento (id,name1, name2, data) VALUES ('a1','Joseh','Marcus','${data}');`
        q1 = execute(query1, []);
        data = new Date("Jan 24, 2017");
        dia = data.getDate();
        if (dia < 10) {
            dia = String("0" + dia);
        }
        data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));
        //console.log(data);
        query2 = `INSERT INTO registro.casamento (id, name1, name2, data) VALUES ('b2','Jhon','Camila','${data}');`
        q2 = execute(query2, []);
        query3 = `INSERT INTO registro.casamento (id, name1, name2) VALUES ('c3','Hiago','Taina');`;
        q3 = execute(query3, []);
        query4 = `INSERT INTO registro.casamento (id, name1) VALUES ('d4','Rodman');`;
        q4 = execute(query4, []);
        promises.push(q1);
        promises.push(q2);
        promises.push(q3);
        promises.push(q4);
        const promisesAll2 = Promise.all(promises);
        promisesAll2.then((result) => {
            Registros([query1, query2, query3, query4]).then(result => {
                data = new Date("Sept 18, 2022");
                dia = data.getDate();
                if (dia < 10) {
                    dia = String("0" + dia);
                }
                data = String((data.getFullYear() + "-" + meses[(data.getMonth())] + "-" + dia));

                query4 = `INSERT INTO registro.casamento (id, data) VALUES ('c3','${data}');`
                q4 = execute(query4, []).then(result => {
                    Registros([query4]).then(result => {
                        query5 = `UPDATE registro.casamento SET data = '${'2022-03-25'}' WHERE id = 'c3';`
                        q5 = execute(query5, []).then(result => {
                            Registros([query5]).then(result => {
                                console.log("");
                                console.log("Querys secundárias feitas");
                            })
                        }).catch(err => { console.log(err) });
                    });
                }).catch(err => { console.log(err) });
            }).catch(err => { console.log(err) });
        }).catch(err => { console.log });
    }).catch(err => { console.log(err); });
}).catch(err => {
    console.log(err)
});
/*
    query = `SELECT name, price_p_item FROM registro.casamento WHERE name=? ALLOW FILTERING`;
    q1 = execute(query, ['oranges']).then(result => {
        console.log('The cost per orange is', Number(result.rows[0].price_p_item));
    }).catch(err => {
        console.log(err)
    });*/