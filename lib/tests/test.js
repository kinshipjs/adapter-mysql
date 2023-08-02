//@ts-check
import { KinshipContext } from "@kinshipjs/core";
import { adapter, createMySql2Pool } from "../src/adapter.js";
import { config } from 'dotenv';

config();
const dbCfg = { 
    database: process.env.DB_DB, 
    host: process.env.DB_HOST, 
    user: process.env.DB_USER, 
    password: process.env.DB_PASS, 
    port: parseInt(process.env.DB_PORT ?? "3306") 
};
async function test() {
    const pool = createMySql2Pool(dbCfg);
    const myAdapter = adapter(pool);
    /** @type {KinshipContext<import('../../../../adapter-tests/chinook-types').Playlist>}*/
    const ctx = new KinshipContext(myAdapter, "Playlist");
    /** @type {KinshipContext<import('../../../../adapter-tests/chinook-types').PlaylistTrack>}*/
    const ctx2 = new KinshipContext(myAdapter, "PlaylistTrack");
    /** @type {KinshipContext<import('../../../../adapter-tests/chinook-types').Track>}*/
    const ctx3 = new KinshipContext(myAdapter, "Track");

    ctx.hasMany(m => m.PlaylistTracks.fromTable("PlaylistTrack").withKeys("PlaylistId", "PlaylistId")
        .andThatHasOne(m => m.Track.withKeys("TrackId", "TrackId")));

    ctx.onSuccess(({ cmdRaw, cmdSanitized }) => {
        console.log(cmdRaw);
    });

    ctx.onFail(({ cmdRaw, cmdSanitized }) => {
        console.log(cmdRaw);
    });

    const records = await ctx.include(m => m.PlaylistTracks.thenInclude(m => m.Track)).select();
    console.log(JSON.stringify(records, undefined, 2));
    process.exit(1);
}

test();