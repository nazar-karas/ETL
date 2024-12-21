using System.Data;
using Microsoft.Data.SqlClient;

 string pathToCsvFile = null;

while (string.IsNullOrEmpty(pathToCsvFile))
{
    Console.WriteLine("Enter path to you CSV file");
    pathToCsvFile = Console.ReadLine();
}

string connectionString = null;

while (string.IsNullOrEmpty(connectionString))
{
    Console.WriteLine("Enter a connection string");
    connectionString = Console.ReadLine();
}

int rowsTaken = 0;
int batchSize = 2000;
string tableNameWithDuplicates = "DataWithDuplicates";
string tableName = "ImportedData";

Console.WriteLine($"Processing records in batches with size of {batchSize} rows");

var columns = new List<string>()
{
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount"
};

var dataTable = new DataTable();
columns.ForEach(c => dataTable.Columns.Add(c));

using (var reader = new StreamReader(pathToCsvFile))
{
    var headers = reader.ReadLine()?.Split(",");

    if (headers == null)
    {
        throw new Exception("CSV file is empty or has no headers.");
    }

    while (!reader.EndOfStream)
    {
        var rowWithAllColumns = reader.ReadLine()?.Split(",");

        if (rowWithAllColumns != null && rowWithAllColumns.Count() != 1)
        {
            var dataRow = dataTable.NewRow();

            foreach (var column in columns)
            {
                int index = Array.IndexOf(headers, column);

                rowWithAllColumns[index] = rowWithAllColumns[index].Trim();

                if (column == "store_and_fwd_flag")
                {
                    dataRow[column] = rowWithAllColumns[index] == "Y" ?
                        "Yes" : rowWithAllColumns[index] == "N" ? "No" :
                        rowWithAllColumns[index];
                }
                else if (Decimal.TryParse(rowWithAllColumns[index], out var decimalValue))
                {
                    dataRow[column] = decimalValue;
                }
                else if (Int32.TryParse(rowWithAllColumns[index], out var intValue))
                {
                    dataRow[column] = intValue;
                }
                else if (DateTime.TryParse(rowWithAllColumns[index], out var estDate))
                {
                    var easternZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
                    var utcDate = TimeZoneInfo.ConvertTimeToUtc(estDate, easternZone);
                    dataRow[column] = utcDate;
                }
                else
                {
                    dataRow[column] = !string.IsNullOrEmpty(rowWithAllColumns[index]) ? rowWithAllColumns[index] : null;
                }
            }

            dataTable.Rows.Add(dataRow);
        }

        rowsTaken++;

        if (rowsTaken == batchSize)
        {
            using (var copy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.KeepNulls))
            {
                copy.DestinationTableName = tableNameWithDuplicates;
                copy.WriteToServer(dataTable);
            }

            dataTable.Clear();
            rowsTaken = 0;
        }
    }

    if (rowsTaken > 0)
    {
        using (var copy = new SqlBulkCopy(connectionString, SqlBulkCopyOptions.KeepNulls))
        {
            copy.DestinationTableName = tableNameWithDuplicates;
            copy.WriteToServer(dataTable);
        }
    }
}

string queryToExportDuplicates = $@"
WITH Deduplicated AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count ORDER BY passenger_count) AS RowNum
    FROM DataWithDuplicates
)
SELECT *
FROM Deduplicated
WHERE RowNum > 1;";

using (SqlConnection connection = new SqlConnection(connectionString))
{
    connection.Open();

    // Execute the query to fetch duplicates
    SqlCommand command = new SqlCommand(queryToExportDuplicates, connection);
    command.CommandTimeout = 300;
    SqlDataReader reader = command.ExecuteReader();

    // Write the data to a CSV file
    using (StreamWriter writer = new StreamWriter("duplicates.csv"))
    {
        // Write header
        writer.WriteLine("tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,store_and_fwd_flag,PULocationID,DOLocationID,fare_amount,tip_amount");

        // Write rows
        while (reader.Read())
        {
            string tpep_pickup_datetime = reader["tpep_pickup_datetime"].ToString();
            string tpep_dropoff_datetime = reader["tpep_dropoff_datetime"].ToString();
            string passenger_count = reader["passenger_count"].ToString();
            string trip_distance = reader["trip_distance"].ToString();
            string store_and_fwd_flag = reader["store_and_fwd_flag"].ToString();
            string PULocationID = reader["PULocationID"].ToString();
            string DOLocationID = reader["DOLocationID"].ToString();
            string fare_amount = reader["fare_amount"].ToString();
            string tip_amount = reader["tip_amount"].ToString();

            writer.WriteLine($"{tpep_pickup_datetime},{tpep_dropoff_datetime},{passenger_count},{trip_distance},{store_and_fwd_flag},{PULocationID},{DOLocationID},{fare_amount},{tip_amount}");
        }
    }

    reader.Close();

    string queryToRemoveDuplicates = $@"
DELETE FROM {tableNameWithDuplicates}
WHERE EXISTS (
    SELECT 1
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count 
                                  ORDER BY passenger_count) AS RowNum
        FROM {tableNameWithDuplicates}
    ) AS Deduplicated
    WHERE Deduplicated.RowNum > 1
    AND Deduplicated.tpep_pickup_datetime = {tableNameWithDuplicates}.tpep_pickup_datetime
    AND Deduplicated.tpep_dropoff_datetime = {tableNameWithDuplicates}.tpep_dropoff_datetime
    AND Deduplicated.passenger_count = {tableNameWithDuplicates}.passenger_count
);";

    using (var command2 = new SqlCommand(queryToRemoveDuplicates, connection))
    {
        command2.CommandTimeout = 300;
        command2.ExecuteNonQuery();
    }

    string queryForFinalTable = $@"
INSERT INTO {tableName}(
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount)
SELECT tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    fare_amount,
    tip_amount
FROM 
    {tableNameWithDuplicates}";

    using (var command3 = new SqlCommand(queryForFinalTable, connection))
    {
        command3.CommandTimeout = 300;
        command3.ExecuteNonQuery();
    }
}

Console.WriteLine("Data was extracted, transformed and loaded to database");
Console.ReadKey();