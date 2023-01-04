class DeltaMgr:
    def __init__(self):
        pass

    def init_props(type="default", table_name="default", database="default", delta_location="/FileStore/Users/tmp") -> dict:
        return {
            "delta_loc": delta_location,
            "write_fmt": 'delta',
            "table_name": table_name,
            "write_mode": 'append', 
            "type": type,
            "database": database
        }

    def update_delta_fs(df, params):
        df.write \
            .format(params["write_fmt"]) \
            .mode(params["write_mode"]) \
            .save(params["delta_loc"])

    #Add support later for partitions cols and unity catalog
    def create_delta_table(df, params, match_col, spark):

        type = params["type"]
        database = params["database"]
        table_name = params["table_name"]
        delta_loc = params["delta_loc"]

        if spark._jsparkSession.catalog().tableExists(database, table_name):
 
            print("table already exists..... appending data")
            df.createOrReplaceTempView(f"vw_{type}")

            spark.sql(f"MERGE INTO {database}.{table_name} USING vw_{type} \
        ON vw_{type}.{match_col} = {database}.{table_name}.{match_col} \
        WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

        else:
            print("table does not exist..... please create it first")

            # Write the data to its target.
            spark.sql("CREATE TABLE " + database + "." + table_name + " USING DELTA LOCATION '" + delta_loc + "'")