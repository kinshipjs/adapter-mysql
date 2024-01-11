// @ts-check
import { createPool } from "mysql2/promise";
import { forQuery } from "./query.js";
import { forInsert } from "./insert.js";
import { forUpdate } from "./update.js";
import { forDelete } from "./delete.js";

export const createMySql2Pool = createPool;

/** @type {import('@kinshipjs/core/adapter').InitializeAdapterCallback<import("mysql2/promise").Pool|import("mysql2/promise").Connection>} */
export function adapter(connection) {
    return {
        syntax: {
            dateString: date => date.getUTCFullYear() + "-" 
                + date.getUTCMonth() + "-" 
                + date.getUTCDate() + "" 
                + date.getUTCHours() + ":"
                + date.getUTCMinutes()
        },
        aggregates: {
            total: "COUNT(*)",
            count: (table, col) => "COUNT(DISTINCT `" + table + "`.`" + col + "`)",
            avg: (table, col) => "AVG(`" + table + "`.`" + col + "`)",
            max: (table, col) => "MAX(`" + table + "`.`" + col + "`)",
            min: (table, col) => "MIN(`" + table + "`.`" + col + "`)",
            sum: (table, col) => "SUM(`" + table + "`.`" + col + "`)"
        },
        execute({ ErrorTypes, transaction }) {
            connection = transaction ?? connection;
            return {
                async forQuery(cmd, args) {
                    try {
                        const [results] = await connection.query(cmd, args);
                        return /** @type {any} */ (results);
                    } catch(err) {
                        throw handleError(err, ErrorTypes);
                    }
                },
                async forInsert(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return Array.from(Array(result.affectedRows).keys()).map((_, n) => n + result.insertId);
                    } catch(err) {
                        throw handleError(err, ErrorTypes);
                    }
                },
                async forUpdate(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return result.affectedRows;
                    } catch(err) {
                        throw handleError(err, ErrorTypes);
                    }
                },
                async forDelete(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return result.affectedRows;
                    } catch(err) {
                        throw handleError(err, ErrorTypes);
                    }
                },
                async forTruncate(cmd, args) {
                    try {
                        const [result] = /** @type {import('mysql2/promise').ResultSetHeader[]} */ (await connection.execute(cmd, args));
                        return result.affectedRows;
                    } catch(err) {
                        throw handleError(err, ErrorTypes);
                    }
                },
                async forDescribe(cmd, args) {
                    const [result] = /** @type {any[]} */ (await connection.execute(cmd, args));
                    /** @type {any} */
                    let set = {};
                    for(const field of result) {
                        let defaultValue = getDefaultValueFn(field.Type, field.Default, field.Extra);
                        let type = field.Type.toLowerCase();
                        
                        loopThroughDataTypes:
                        for (const dataType in mysqlDataTypes) {
                            for(const dt of mysqlDataTypes[dataType]) {
                                if(type.startsWith(dt)) {
                                    type = dataType;
                                    break loopThroughDataTypes;
                                }
                            }
                        }
                        set[field.Field] = {
                            field: field.Field,
                            table: "",
                            alias: "",
                            isPrimary: field.Key === "PRI",
                            isIdentity: field.Extra.includes("auto_increment"),
                            isVirtual: field.Extra.includes("VIRTUAL"),
                            isNullable: field.Null === "YES",
                            datatype: type,
                            defaultValue
                        };
                    }
                    return set;
                },
                async forTransaction() {
                    return {
                        begin: async () => {
                            let transaction;
                            if("getConnection" in connection) {
                                transaction = await connection.getConnection();
                            } else {
                                transaction = connection;
                            }
                            await transaction.beginTransaction();
                            
                            return transaction;
                        },
                        commit: async (transaction) => {
                            await transaction.commit();
                        },
                        rollback: async (transaction) => {
                            await transaction.rollback();
                        }
                    };
                }
            }
        },
        serialize() {
            return {
                forQuery,
                forInsert,
                forUpdate,
                forDelete,
                forTruncate(data) {
                    return { cmd: "TRUNCATE " + data.table + ";", args: [] };
                },
                forDescribe(table) {
                    return { cmd: "DESCRIBE " + table + ";", args: [] };
                }
            }
        }
    }
}

/**
 * Handles any error thrown from the database library and throws it as a Kinship error instead.
 * @param {Error & { errno: number }} originalError 
 * @param {import('@kinshipjs/core/errors').ErrorType} error
 * @returns {Error}
 */
function handleError(originalError, { 
    NonUniqueKey, 
    ValueCannotBeNull, 
    UpdateConstraintError, 
    DeleteConstraintError,
    UnknownDBError,
    UnhandledDBError
}) {
    switch(originalError.errno) {
        // required to pass @kinshipjs/adapter-tests
        case 1062: throw NonUniqueKey(originalError.errno, originalError.message);
        case 1048: throw ValueCannotBeNull(originalError.errno, originalError.message);
        case 1169: throw NonUniqueKey(originalError.errno, originalError.message);
        case 1216: throw UpdateConstraintError(originalError.errno, originalError.message);
        case 1217: throw DeleteConstraintError(originalError.errno, originalError.message);

        // recommended, but not required
        case 1105: throw UnknownDBError(`Unknown database error occurred.`, originalError.errno, originalError.message);

        // does not need to be handled, but can be if you want to give more context to the user on why things may have errored.
        case 1053: throw UnhandledDBError(`Server shutting down.`, originalError.errno, originalError.message);
        case 1065: throw UnhandledDBError(`Parse error.`, originalError.errno, originalError.message);
        case 1180: throw UnhandledDBError(`Error during commit.`, originalError.errno, originalError.message);

        // likely would be a problem within @kinshipjs/core itself and should be addressed as an issue.
        case 1055: throw UnhandledDBError(`Wrong field used with GROUP BY.`, originalError.errno, originalError.message);
        case 1057: throw UnhandledDBError(`Combination of fields and aggregate sum.`, originalError.errno, originalError.message);
        case 1059: throw UnhandledDBError(`Field name is too long.`, originalError.errno, originalError.message);
        case 1060: throw UnhandledDBError(`Duplicate field name.`, originalError.errno, originalError.message);
        case 1066: throw UnhandledDBError(`Non-unique table name.`, originalError.errno, originalError.message);

        // any of the above from 1053 to here could also just be optionally handled here.
        default: throw UnhandledDBError(`Unhandled error.`, originalError.errno, originalError.message);
    }
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

// gets the default value callback function for a given column.
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