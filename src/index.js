// @ts-check
import { createPool } from "mysql2/promise";

/** @type {import('@kinshipjs/core/adapter').InitializeAdapterCallback<import("mysql2/promise").Pool|import("mysql2/promise").Connection>} */
export function adapter(connection) {
    let transactionConnection;
    return {
        syntax: {
            dateString: (date) => `${date.getUTCFullYear()}-${date.getUTCMonth()}-${date.getUTCDate()} ${date.getUTCHours}:${date.getUTCMinutes()}`
        },
        aggregates: {
            total: "COUNT(*)",
            count: (table, col) => `COUNT(DISTINCT \`${table}\`.\`${col}\`)`,
            avg: (table, col) => `AVG(\`${table}\`.\`${col}\`)`,
            max: (table, col) => `MAX(\`${table}\`.\`${col}\`)`,
            min: (table, col) => `MIN(\`${table}\`.\`${col}\`)`,
            sum: (table, col) => `SUM(\`${table}\`.\`${col}\`)`
        },
        execute(scope) {
            return {
                async forQuery(cmd, args) {
                    try {
                        const [results] = await connection.query(cmd, args);
                        return /** @type {any} */ (results);
                    } catch(err) {
                        await transactionConnection.rollback();
                        throw handleError(err);
                    }
                },
                async forInsert(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return Array.from(Array(result.affectedRows).keys()).map((_, n) => n + result.insertId);
                    } catch(err) {
                        await transactionConnection.rollback();
                        throw handleError(err);
                    }
                },
                async forUpdate(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return result.affectedRows;
                    } catch(err) {
                        await transactionConnection.rollback();
                        throw handleError(err);
                    }
                },
                async forDelete(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return result.affectedRows;
                    } catch(err) {
                        await transactionConnection.rollback();
                        throw handleError(err);
                    }
                },
                async forTruncate(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return result.affectedRows;
                    } catch(err) {
                        await transactionConnection.rollback();
                        throw handleError(err);
                    }
                },
                async forDescribe(cmd, args) {
                    const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                    /** @type {any} */
                    let set = {}
                    for(const field in result) {
                        let defaultValue = getDefaultValueFn(result[field].Type, result[field].Default, result[field].Extra);
                        let type = result[field].Type.toLowerCase();
                        
                        loopThroughDataTypes:
                        for (const dataType in mysqlDataTypes) {
                            for(const dt of mysqlDataTypes[dataType]) {
                                if(type.startsWith(dt)) {
                                    type = dataType;
                                    break loopThroughDataTypes;
                                }
                            }
                        }
                        set[field] = {
                            field: result[field].Field,
                            table: "",
                            alias: "",
                            isPrimary: result[field].Key === "PRI",
                            isIdentity: result[field].Extra.includes("auto_increment"),
                            isVirtual: result[field].Extra.includes("VIRTUAL"),
                            isNullable: result[field].Null === "YES",
                            datatype: type,
                            defaultValue
                        };
                    }
                    return set;
                },
                async forTransactionBegin() {
                    //@ts-ignore
                    transactionConnection = await connection.getConnection();
                    await transactionConnection.beginTransaction();
                    return transactionConnection;
                },
                async forTransactionEnd(cnn) {
                    await transactionConnection.commit();
                }
            }
        },
        serialize() {
            return {
                forQuery(data) {
                    const selects = getSelects(data.select);
                    const from = getFrom(data.from, data.limit, data.offset);
                    const where = getWhere(data.where);
                    const groupBy = getGroupBy(data.group_by);
                    const orderBy = getOrderBy(data.order_by);
                    const limit = getLimit(data.limit);
                    const offset = getOffset(data.offset);
                    /** @type {any[]} */
                    let args = [];
                    let cmd = "";
                    if(data.from.length > 1) {
                        cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${groupBy.cmd}${orderBy.cmd}`;
                        /** @type {any[]} */
                        args = args.concat(...[
                            selects.args,
                            from.args, 
                            where.args,
                            groupBy.args, 
                            orderBy.args
                        ]);
                    } else {
                        cmd = `SELECT ${selects.cmd}\n\tFROM ${from.cmd}${where.cmd}${limit.cmd}${offset.cmd}${groupBy.cmd}${orderBy.cmd}`;
                        args = args.concat(...[
                            selects.args, 
                            from.args, 
                            where.args,
                            limit.args, 
                            offset.args, 
                            groupBy.args, 
                            orderBy.args
                        ]);
                    }

                    return { 
                        cmd, 
                        args
                    };
                },
                forInsert(data) {
                    let cmd = "";
                    let args = [];
                    const { table, columns, values } = data;
                    args = values.flat();
                    cmd += `INSERT INTO ${table} (${columns.join(', ')})\n\tVALUES\n\t\t${values.flatMap(v => `(${Array.from(Array(v.length).keys()).map(_ => '?')})`).join('\n\t\t,')}`;
                    return { cmd: cmd, args: args };
                },
                forUpdate(data) {
                    const { table, columns, where, explicit, implicit } = data;
                    const { cmd: explicitCmd, args: explicitArgs } = getExplicitUpdate({ table, columns, where, explicit });
                    const { cmd: implicitCmd, args: implicitArgs } = getImplicitUpdate({ table, columns, where, implicit });
                    return { 
                        cmd: explicitCmd !== '' ? explicitCmd : implicitCmd,
                        args: explicitCmd !== '' ? explicitArgs : implicitArgs
                    };
                },
                forDelete(data) {
                    const { table, where } = data;
                    const { cmd, args } = handleWhere(where);
                    return { cmd: `DELETE FROM ${table} ${cmd}`, args };
                },
                forTruncate(data) {
                    return { cmd: `TRUNCATE ${data.table}`, args: [] };
                },
                forDescribe(table) {
                    return { cmd: `DESCRIBE ${table};`, args: [] };
                }
            }
        }
    }
}

/**
 * 
 * @param {Error} originalError 
 * @returns {Error}
 */
function handleError(originalError) {
    return originalError;
}

// Use {stringToCheck}.startsWith({dataType}) where {dataType} is one of the data types in the array for the respective data type used in Kinship.
// e.g., let determinedDataType = mysqlDataTypes.string.filter(dt => s.startsWith(dt)).length > 0 ? "string" : ...
const mysqlDataTypes = {
    string: [
        "char", "varchar", 
        "binary", "varbinary",
        "tinyblob", "mediumblob", "longblob", "blob",
        "tinytext", "mediumtext", "longtext", "text",
        "enum",
        "set"
    ],
    int: [
        "tinyint", "smallint", "mediumint", "bigint", "int",
    ],
    float: [
        "float",
        "double",
        "decimal",
        "dec"
    ],
    boolean: [
        "bit(1)",
        "bool",
        "boolean"
    ],
    date: [
        "date",
        "time",
        "year"
    ]
};

function handleWhere(conditions, table="", sanitize=(n) => `?`) {
    if(!conditions) return { cmd: '', args: [] };
    let args = [];

    // function to filter out conditions that do not belong to table.
    // (this must be a map, as if it was just filter being used, then it would remove an entire subarray, when maybe that array has conditions)
    const mapFilter = (x) => {
        if(Array.isArray(x)) {
            const filtered = x.map(mapFilter).filter(x => x !== undefined);
            return filtered.length > 0 ? filtered : undefined;
        }
        if(x.table.includes(table)) {
            return x;
        }
        return undefined;
    }

    // function to reduce each condition to one appropriate clause string.
    const reduce = (prevStr, cond, depth=0) => {
        const tabs = Array.from(Array(depth + 2).keys()).map(_ => `\t`).join('');
        
        // nested conditions
        if(Array.isArray(cond)) {
            const [nextCond, ...remainder] = cond;

            // edge case: BETWEEN operator.
            if(nextCond.operator === "BETWEEN") {
                const column = `\`${nextCond.table}\`.\`${nextCond.property}\``;
                const lowerBound = sanitize(args.length);
                const upperBound = sanitize(args.length+1);
                const reduceStart = `${nextCond.chain} (${column} ${nextCond.operator} ${lowerBound} AND ${upperBound}`;
                const reduced = remainder.reduce((a, b) => reduce(a, b, depth + 1), reduceStart);
                const cmd = `${prevStr} ${reduced})\n${tabs}`;
                args = args.concat(nextCond.value);
                return cmd;
            }
            let value;
            if (Array.isArray(nextCond.value)) {
                args = args.concat(nextCond.value);
                value = `(${nextCond.value.map((_,n) => sanitize(args.length+n)).join(',')})`;
            } else {
                args.push(nextCond.value);
                value = sanitize(args.length);
            }
            const column = `\`${nextCond.table}\`.\`${nextCond.property}\``;
            const reduceStart = `${nextCond.chain} (${column} ${nextCond.operator} ${value}`;
            const reduced = remainder.reduce((a, b) => {
                return reduce(a, b, depth + 1);
            }, reduceStart);
            const cmd = `${prevStr} ${reduced})\n${tabs}`;
            return cmd;
        }
        
        // edge case: BETWEEN operator.
        if(cond.operator === "BETWEEN") {
            const column = `\`${cond.table}\`.\`${cond.property}\``;
            const lowerBound = sanitize(args.length);
            const upperBound = sanitize(args.length+1);
            const cmd = prevStr + `${cond.chain} ${column} ${cond.operator} ${lowerBound} AND ${upperBound}\n${tabs}`;
            args = args.concat(cond.value);
            return cmd;
        }

        // single condition.
        let value;
        if (Array.isArray(cond.value)) {
            args = args.concat(cond.value);
            value = `(${cond.value.map((_, n) => sanitize(args.length + n)).join(',')})`;
        } else {
            args.push(cond.value);
            value = sanitize(args.length);
        }
        const column = `\`${cond.table}\`.\`${cond.property}\``;
        const cmd = prevStr + `${cond.chain} ${column} ${cond.operator} ${value}\n${tabs}`;
        return cmd;
    };
    
    // map the array, filter out undefineds, then reduce the array to get the clause.
    /** @type {string} */
    const reduced = conditions.map(mapFilter).filter(x => x !== undefined).reduce(reduce, '');
    return {
        // if a filter took place, then the WHERE statement of the clause may not be there, so we replace.
        cmd: reduced.startsWith("WHERE") 
            ? reduced.trimEnd()
            : reduced.startsWith("AND") 
                ? reduced.replace("AND", "WHERE").trimEnd() 
                : reduced.replace("OR", "WHERE").trimEnd(),
        // arguments was built inside the reduce function.
        args
    };
}

function getDefaultValueFn(type, defaultValue, extra) {
    if(extra.includes("DEFAULT_GENERATED")) {
        switch(defaultValue) {
            case "CURRENT_TIMESTAMP": {
                return () => new Date;
            }
        }
    }
    if(defaultValue !== null) {
        if(type.includes("tinyint")) {
            defaultValue = parseInt(defaultValue) === 1;
        } else if(type.includes("bigint")) {
            defaultValue = BigInt(defaultValue);
        } else if(type.includes("double")) {
            defaultValue = parseFloat(defaultValue);
        } else if(type.includes("date")) {
            defaultValue = Date.parse(defaultValue);
        } else if(type.includes("int")) {
            defaultValue = parseInt(defaultValue);
        }
    }
    return () => defaultValue;
}

function getSelects(select) {
    const cols = select.map(prop => {
        if(prop.alias === '') {
            return ``;
        }
        if(!("aggregate" in prop)) {
            return `\`${prop.table}\`.\`${prop.column}\` AS \`${prop.alias}\``;
        }
        return `${prop.column} AS \`${prop.alias}\``;
    }).join('\n\t\t,');
    return {
        cmd: `${cols}`,
        args: []
    };
}

function getLimit(limit) {
    if(!limit) return { cmd: "", args: [] };
    return {
        cmd: `\n\tLIMIT ?`,
        args: [limit]
    };
}

function getOffset(offset) {
    if(!offset) return { cmd: "", args: [] };
    return {
        cmd: `\n\tOFFSET ?`,
        args: [offset]
    };
}

function getFrom(from, limit, offset) {
    let cmd = "";
    let args = [];
    if(from.length > 1) {
        const joiningTables = [];
        const [main, ...joins] = from;
        if(limit) {
            const limitCmd = getLimit(limit);
            const offsetCmd = getOffset(offset);
            const mainSubQuery = `(SELECT * FROM \`${main.realName}\` ${limitCmd.cmd} ${offsetCmd.cmd}) AS \`${main.alias}\``;
            args = args.concat(limitCmd.args, offsetCmd.args);
            joiningTables.push(mainSubQuery);
        } else {
            joiningTables.push(`\`${main.realName}\` AS \`${main.alias}\``);
        }

        cmd = joiningTables.concat(joins.map(table => {
            const nameAndAlias = `\`${table.realName}\` AS \`${table.alias}\``;
            const onRefererKey = `\`${table.refererTableKey.table}\`.\`${table.refererTableKey.column}\``;
            const onReferenceKey = `\`${table.referenceTableKey.table}\`.\`${table.referenceTableKey.column}\``;
            return `${nameAndAlias}\n\t\t\tON ${onRefererKey} = ${onReferenceKey}`;
        })).join('\n\t\tLEFT JOIN');
    } else {
        cmd = `\`${from[0].realName}\` AS \`${from[0].alias}\``
    }
    return { cmd, args };
}

function getGroupBy(group_by) {
    if(!group_by) return { cmd: "", args: [] };
    return {
        cmd: '\n\tGROUP BY ' + group_by.map(prop => `\`${prop.alias}\``).join('\n\t\t,'),
        args: []
    };
}

function getOrderBy(order_by) {
    if(!order_by) return { cmd: "", args: [] };
    return {
        cmd: '\n\tORDER BY ' + order_by.map(prop => prop.alias).join('\n\t\t,'),
        args: []
    };
}

function getWhere(where) {
    if(!where) return { cmd: "", args: [] };
    const whereInfo = handleWhere(where);
    return {
        cmd: `\n\t${whereInfo.cmd}`,
        args: whereInfo.args
    };
}

/**
 * @param {any} param0 
 * @returns 
 */
function getExplicitUpdate({ table, columns, where, explicit }) {
    if(!explicit) return { cmd: "", args: "" };
    const { values } = explicit;
    const { cmd: cmdWhere, args: cmdArgs } = getWhere(where);

    const setValues = `\n\t\t${values.map((v,n) => `${columns[n]} = ?`).join('\n\t\t,')}`;
    return {
        cmd: `UPDATE ${table}\n\tSET${setValues}${cmdWhere}`,
        args: values.concat(cmdArgs)
    }
}

/**
 * @param {any} param0 
 * @returns 
 */
function getImplicitUpdate({ table, columns, where, implicit }) {
    if(!implicit) { 
        return { cmd: "", args: [] };
    }
    const { primaryKeys, objects } = implicit;

    // initialize all of the cases.
    let cases = columns.reduce(
        (prev, initial) => ({ ...prev, [initial]: { cmd: 'CASE\n\t\t', args: [] }}), 
        {}
    );
    // set each column in a case when (Id = ?) statement.
    for (const record of objects) {
        for (const key in record) {
            for(const primaryKey of primaryKeys) {
                // ignore the primary key, we don't want to set that.
                if(key === primaryKey) continue;
                cases[key].cmd += `\tWHEN ${primaryKey} = ? THEN ?\n\t\t`;
                cases[key].args = [...cases[key].args, record[primaryKey], record[key]];
            }
        }
    }
    // finish each case command.
    Object.keys(cases).forEach(k => cases[k].cmd += `\tELSE \`${k}\`\n\t\tEND`);

    // delete the cases that have no sets. (this covers the primary key that we skipped above.)
    for (const key in cases) {
        if (cases[key].args.length <= 0) {
            delete cases[key];
        }
    }
    const { cmd: cmdWhere, args: cmdArgs } = getWhere(where);
    return {
        cmd: `UPDATE ${table}\n\tSET\n\t\t${Object.keys(cases).map(k => `\`${k}\` = (${cases[k].cmd})`).join(',\n\t\t')}${cmdWhere}`,
        args: [...Object.keys(cases).flatMap(k => cases[k].args), ...cmdArgs]
    };
}

export const createMySql2Pool = createPool;
