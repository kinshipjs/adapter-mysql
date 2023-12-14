//@ts-check
import { KinshipContext, transaction } from '@kinshipjs/core';
import { adapter, createMySql2Pool } from '../src/index.js';
import crypto from 'crypto';

const pool = createMySql2Pool({
    database: "kinship_test",
    host: "192.168.1.28",
    user: "root",
    password: "root",
    port: 10500
});

const connection = adapter(pool);
/** @type {KinshipContext<import('../../core/test/test.js').User>} */
const ctx = new KinshipContext(connection, "User");

ctx.beforeInsert(m => {
    m.Id = crypto.randomUUID().slice(0,6);
});

const id = crypto.randomUUID().slice(0,6);
try {
    const user = await transaction(connection).execute(async tnx => {
        const users = ctx.using(tnx);
        const [user] = await users.insert({ Id: id, FirstName: "John", LastName: "Doe" });
        const n = await users.where(m => m.Id.equals(user.Id)).delete();
    
        console.log(n);
        return user;
    });
} catch(err) {

}

const [johnDoe] = await ctx.where(m => m.Id.equals(id));
console.log({johnDoe});