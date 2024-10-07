using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;

class SchemaConverter
{
    static string GetType(string mssqlType)
    {
        switch (mssqlType)
        {
            case "nvarchar":
                return "varchar";
            case "tinyint":
                return "smallint";
            case "bigint":
                return "bigint";
            case "int":
                return "int";
            case "sysname":
                return "varchar";
            case "xml":
                return "xml";
            case "float":
                return "double precision";
            case "uniqueidentifier":
                return "uuid";
            case "geography":
                return "geography";
            case "varbinary":
                return "bytea";
            case "smalldatetime":
                return "timestamp";
            default:
                return mssqlType;
        }
    }
    static void Main()
    {
        DateTime currentTime = DateTime.Now;
        Console.WriteLine($"Starting schema conversion at {currentTime}...");
        try
        {
            string mssqlConnectionString = ConfigurationManager.ConnectionStrings["MSSQLConnectionString"].ConnectionString;
            string postgresqlConnectionString = ConfigurationManager.ConnectionStrings["PostgreSQLConnectionString"].ConnectionString;

            using (SqlConnection mssqlConnection = new SqlConnection(mssqlConnectionString))
            {
                mssqlConnection.Open();
                Console.WriteLine("MSSQL connection open...");
                DataTable schemaTable = mssqlConnection.GetSchema("Tables");
                using (NpgsqlConnection postgresqlConnection = new NpgsqlConnection(postgresqlConnectionString))
                {
                    postgresqlConnection.Open();
                    Console.WriteLine("PGSQL connection open...");

                    // Get list of existing tables in PostgreSQL
                    DataTable pgTables = postgresqlConnection.GetSchema("Tables");
                    HashSet<string> existingTables = new HashSet<string>(pgTables.Rows.Cast<DataRow>().Select(row => row["table_name"].ToString()));

                    Parallel.ForEach(schemaTable.Rows.Cast<DataRow>(), row =>
                    {
                        string tableName = row["TABLE_NAME"].ToString().ToLower();

                        // Skip table if it already exists in PostgreSQL
                        if (existingTables.Contains(tableName))
                        {
                            Console.WriteLine($"{tableName} already exists, skipping...");
                            return;
                        }

                        using (SqlConnection mssqlConn = new SqlConnection(mssqlConnectionString))
                        using (NpgsqlConnection postgresqlConn = new NpgsqlConnection(postgresqlConnectionString))
                        {
                            mssqlConn.Open();
                            postgresqlConn.Open();

                            string createTableQuery = $"CREATE TABLE {tableName} (";
                            DataTable columns = mssqlConn.GetSchema("Columns", new string[] { null, null, tableName, null });
                            foreach (DataRow column in columns.Rows)
                            {
                                string columnName = column["COLUMN_NAME"].ToString();
                                string dataType = column["DATA_TYPE"].ToString();
                                string isNullable = column["IS_NULLABLE"].ToString() == "YES" ? "NULL" : "NOT NULL";
                                string pgDataType = GetType(dataType);

                                if (columnName.ToUpper() == "OFFSET")
                                {
                                    columnName = "\"" + columnName + "\"";
                                }

                                createTableQuery += $"{columnName} {pgDataType} {isNullable}, ";
                            }

                            createTableQuery = createTableQuery.TrimEnd(',', ' ') + ");";
                            using (NpgsqlCommand command = new NpgsqlCommand(createTableQuery, postgresqlConn))
                            {
                                var count = command.ExecuteNonQuery();
                                Console.WriteLine(tableName + " table created...");
                            }

                            // Create indexes and constraints
                            CreateIndexesAndConstraints(mssqlConn, postgresqlConn, tableName);
                        }
                    });
                }
            }
            Console.WriteLine($"Schema conversion completed successfully at {DateTime.Now}.");
            Console.WriteLine($"Time taken-----> {DateTime.Now - currentTime}.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred: " + ex.Message);
        }
        finally
        {
            Console.WriteLine("Press any key to close...");
            Console.ReadKey();
        }
    }

    static void CreateIndexesAndConstraints(SqlConnection mssqlConnection, NpgsqlConnection postgresqlConnection, string tableName)
    {
        // Create Primary Keys
        DataTable primaryKeys = mssqlConnection.GetSchema("IndexColumns", new string[] { null, null, tableName, null });
        foreach (DataRow pk in primaryKeys.Rows)
        {
            if (pk["KeyType"].ToString() == "PRIMARY KEY")
            {
                string pkName = pk["constraint_name"].ToString();
                string columnName = pk["column_name"].ToString();
                string createPkQuery = $"ALTER TABLE {tableName} ADD CONSTRAINT {pkName} PRIMARY KEY ({columnName});";
                using (NpgsqlCommand command = new NpgsqlCommand(createPkQuery, postgresqlConnection))
                {
                    command.ExecuteNonQuery();
                    //Console.WriteLine($"PrimaryKey {pkName} constraint added for table {tableName} to column {columnName}...");
                }
            }
        }

        // Create Foreign Keys
        DataTable foreignKeys = mssqlConnection.GetSchema("ForeignKeys", new string[] { null, null, tableName, null });
        foreach (DataRow fk in foreignKeys.Rows)
        {
            string fkName = fk["constraint_name"].ToString();
            string columnName = fk["column_name"].ToString();
            string referencedTableName = fk["referenced_table_name"].ToString();
            string referencedColumnName = fk["referenced_column_name"].ToString();
            string createFkQuery = $"ALTER TABLE {tableName} ADD CONSTRAINT {fkName} FOREIGN KEY ({columnName}) REFERENCES {referencedTableName} ({referencedColumnName});";
            using (NpgsqlCommand command = new NpgsqlCommand(createFkQuery, postgresqlConnection))
            {
                command.ExecuteNonQuery();
                //Console.WriteLine($"ForeignKey {fkName} constraint added for table {tableName} to column {columnName}...");
            }
        }

        // Create Unique Constraints
        DataTable uniqueConstraints = mssqlConnection.GetSchema("IndexColumns", new string[] { null, null, tableName, null });
        foreach (DataRow uc in uniqueConstraints.Rows)
        {
            if (uc["KeyType"].ToString() == "UNIQUE")
            {
                string ucName = uc["constraint_name"].ToString();
                string columnName = uc["column_name"].ToString();
                string createUcQuery = $"ALTER TABLE {tableName} ADD CONSTRAINT {ucName} UNIQUE ({columnName});";
                using (NpgsqlCommand command = new NpgsqlCommand(createUcQuery, postgresqlConnection))
                {
                    command.ExecuteNonQuery();
                    //Console.WriteLine($"Unique constraint {ucName} added for table {tableName} to column {columnName}...");
                }
            }
        }

        // Create Indexes
        DataTable indexes = mssqlConnection.GetSchema("Indexes", new string[] { null, null, tableName, null });
        foreach (DataRow index in indexes.Rows)
        {
            string indexName = index["index_name"].ToString();
            string typeDesc = index["type_desc"].ToString();

            // Retrieve columns involved in the index
            DataTable indexColumns = mssqlConnection.GetSchema("IndexColumns", new string[] { null, null, tableName, indexName });
            string columns = string.Join(", ", indexColumns.Rows.Cast<DataRow>().Select(row => row["column_name"].ToString()));

            string createIndexQuery = $"CREATE INDEX {indexName} ON {tableName} ({columns});";
            using (NpgsqlCommand command = new NpgsqlCommand(createIndexQuery, postgresqlConnection))
            {
                command.ExecuteNonQuery();
                //Console.WriteLine($"Index created {indexName} for table {tableName} on columns {columns}...");
            }
        }
    }
}