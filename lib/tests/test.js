//@ts-check
import { KinshipContext } from "@kinshipjs/core";
import { adapter, createMySql2Pool } from "../src/adapter.js";
import { config } from 'dotenv';
import { mysqlTable, int, text, float } from 'drizzle-orm/mysql-core';
import { drizzle } from 'drizzle-orm/mysql2';
import { eq, relations } from 'drizzle-orm';

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
    const db = drizzle(pool);
    const myAdapter = adapter(pool);
    /** @type {KinshipContext<import('../../../../adapter-tests/chinook-types').Playlist>}*/
    const ctx = new KinshipContext(myAdapter, "Playlist");

    ctx.onSuccess(({ cmdRaw, cmdSanitized }) => {
        console.log(cmdRaw);
    });

    const playlists = mysqlTable('Playlist', {
        id: int('PlaylistId').primaryKey(),
        name: text('Name'),
    });

    const tracks = mysqlTable('Track', {
        id: int('TrackId').primaryKey(),
        name: text('Name').notNull(),
        albumId: int('AlbumId').notNull(),
        mediaTypeId: int('MediaTypeId').notNull(),
        genreId: int('GenreId').notNull(),
        composer: text('Composer'),
        milliseconds: int('Milliseconds').notNull(),
        bytes: int('Bytes').notNull(),
        unitPrice: float('UnitPrice').notNull()
    });

    const playlistTracks = mysqlTable('PlaylistTrack', {
        playlistId: int('PlaylistId').references(() => playlists.id),
        trackId: int('TrackId').references(() => tracks.id)
    });

    ctx.hasMany(m => m.PlaylistTracks.fromTable("PlaylistTrack").withKeys("PlaylistId", "PlaylistId")
        .andThatHasOne(m => m.Track.withKeys("TrackId", "TrackId"))
    );

    const includedCtx = ctx.include(m => m.PlaylistTracks.thenInclude(m => m.Track));
    await ctx._promise;
    
    async function kinshipBench() {
        const start = microsecondsNow();
        const results = await includedCtx.select();
        const end = microsecondsNow();
        // console.log(JSON.stringify(results, undefined, 2));
        return (end - start) / 1000;
    }

    async function drizzleBench() {
        const start = microsecondsNow();
        const results = await db.select().from(playlists)
            .leftJoin(playlistTracks, eq(playlists.id, playlistTracks.playlistId))
            .leftJoin(tracks, eq(playlistTracks.trackId, tracks.id));
        const end = microsecondsNow();
        // console.log(JSON.stringify(results, undefined, 2));
        return (end - start) / 1000;
    }

    

    // ctx.onFail(({ cmdRaw, cmdSanitized }) => {
    //     console.log(cmdRaw);
    // });

    const total = 100;
    let totalKinship = 0;
    let totalDrizzle = 0;
    let minWaitTimeKinship = 2 ** 53;
    let minWaitTimeDrizzle = 2 ** 53;
    let maxWaitTimeKinship = 0;
    let maxWaitTimeDrizzle = 0;
    let firstRunKs = 0;
    let lastRunKs = 0;
    let firstRunDrz = 0;
    let lastRunDrz = 0;
    for(let i = 0; i < total; ++i) {
        const kinshipRunTime = await kinshipBench();
        if(kinshipRunTime > maxWaitTimeKinship) {
            maxWaitTimeKinship = kinshipRunTime;
        }
        if(kinshipRunTime < minWaitTimeKinship) {
            minWaitTimeKinship = kinshipRunTime;
        }
        totalKinship += kinshipRunTime;

        if(i === 0) {
            firstRunKs = kinshipRunTime;
        }
        if(i === total-1) {
            lastRunKs = kinshipRunTime;
        }
    }
    for(let i = 0; i < total; ++i) {
        const drizzleRunTime = await drizzleBench();
        if(drizzleRunTime > maxWaitTimeDrizzle) {
            maxWaitTimeDrizzle = drizzleRunTime;
        }
        if(drizzleRunTime < minWaitTimeDrizzle) {
            minWaitTimeDrizzle = drizzleRunTime;
        }
        totalDrizzle += drizzleRunTime;

        if(i === 0) {
            firstRunDrz = drizzleRunTime;
        }
        if(i === total-1) {
            lastRunDrz = drizzleRunTime;
        }
    }

    console.log(`@kinshipjs: Query 8719 records (${total} runs) 
        - Total: ${totalKinship.toFixed(6)}ms, 
        - Average: ${(totalKinship / total).toFixed(6)}ms
        - Minimum: ${minWaitTimeKinship.toFixed(6)}ms
        - Maximum: ${maxWaitTimeKinship.toFixed(6)}ms
        - First Run: ${firstRunKs.toFixed(6)}ms
        - Last Run: ${lastRunKs.toFixed(6)}ms`);
    console.log(`drizzle-orm: Query 8719 records (${total} runs) 
        - Total: ${totalDrizzle.toFixed(6)}ms, 
        - Average: ${(totalDrizzle / total).toFixed(6)}ms
        - Minimum: ${minWaitTimeDrizzle.toFixed(6)}ms
        - Maximum: ${maxWaitTimeDrizzle.toFixed(6)}ms
        - First Run: ${firstRunDrz.toFixed(6)}ms
        - Last Run: ${lastRunDrz.toFixed(6)}ms`);
    
    process.exit(1);
}

function microsecondsNow() {
    const hrTime = process.hrtime();
    return hrTime[0] * 1000000 + hrTime[1] / 1000;
}

test();