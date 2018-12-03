import {DriverPackageNotInstalledError} from "../../error/DriverPackageNotInstalledError";
import {DriverOptionNotSetError} from "../../error/DriverOptionNotSetError";
import {PlatformTools} from "../../platform/PlatformTools";
import {Connection} from "../../connection/Connection";
import {SqliteConnectionOptions} from "./SqliteConnectionOptions";
import {ColumnType} from "../types/ColumnTypes";
import {QueryRunner} from "../../query-runner/QueryRunner";
import {AbstractSqliteDriver} from "../sqlite-abstract/AbstractSqliteDriver";
import {BetterSqliteQueryRunner} from "./BetterSqliteQueryRunner";
import * as BetterSqlite from "better-sqlite3";

/**
 * Organizes communication with sqlite DBMS.
 */
export class BetterSqliteDriver extends AbstractSqliteDriver {

    // -------------------------------------------------------------------------
    // Public Properties
    // -------------------------------------------------------------------------

    /**
     * Connection options.
     */
    options: SqliteConnectionOptions;

    /**
     * SQLite underlying library.
     */
    sqlite: BetterSqlite;

    databaseConnection: ReturnType<typeof BetterSqlite>;

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    constructor(connection: Connection) {
        super(connection);

        this.connection = connection;
        this.options = connection.options as SqliteConnectionOptions;
        this.database = this.options.database;

        // validate options to make sure everything is set
        if (!this.options.database)
            throw new DriverOptionNotSetError("database");

        // load sqlite package
        this.loadDependencies();
    }

    // -------------------------------------------------------------------------
    // Public Methods
    // -------------------------------------------------------------------------

    /**
     * Closes connection with database.
     */
    async disconnect(): Promise<void> {
        return new Promise<void>((ok, fail) => {
            this.queryRunner = undefined;
            try {
                this.databaseConnection.close();
                ok();
            } catch (err) {
                fail(err);
            }
        });
    }

    /**
     * Creates a query runner used to execute database queries.
     */
    createQueryRunner(mode: "master"|"slave" = "master"): QueryRunner {
        if (!this.queryRunner)
            this.queryRunner = new BetterSqliteQueryRunner(this);

        return this.queryRunner;
    }

    normalizeType(column: { type?: ColumnType, length?: number | string, precision?: number|null, scale?: number }): string {
        if ((column.type as any) === Buffer) {
            return "blob";
        }

        return super.normalizeType(column);
    }

    // -------------------------------------------------------------------------
    // Protected Methods
    // -------------------------------------------------------------------------

    /**
     * Creates connection with the database.
     */
    protected createDatabaseConnection() {
        return new Promise<void>(async (ok, fail) => {
            await this.createDatabaseDirectory(this.options.database);
            try {
                const databaseConnection = new BetterSqlite(this.options.database);

                // we need to enable foreign keys in sqlite to make sure all foreign key related features
                // working properly. this also makes onDelete to work with sqlite.
                databaseConnection.pragma(`foreign_keys = ON;`);

                // in the options, if encryption key for for SQLCipher is setted.
                if (this.options.key) {
                    databaseConnection.pragma(`key = ${this.options.key};`);
                }
            } catch (err) {
                fail(err);
            }
        });
    }

    /**
     * If driver dependency is not given explicitly, then try to load it via "require".
     */
    protected loadDependencies(): void {
        try {
            this.sqlite = PlatformTools.load("better-sqlite3").verbose();

        } catch (e) {
            throw new DriverPackageNotInstalledError("SQLite", "better-sqlite3");
        }
    }

    /**
     * Auto creates database directory if it does not exist.
     */
    protected createDatabaseDirectory(fullPath: string): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            const mkdirp = PlatformTools.load("mkdirp");
            const path = PlatformTools.load("path");
            mkdirp(path.dirname(fullPath), (err: any) => err ? reject(err) : resolve());
        });
    }

}
