from pg8000.native import Connection
"""added"""

def lambda_handler(event, context):
    """Function to form connection to the totesys database"""

    connection = Connection(
                    "project_team_5",
                    host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
                    password="EE7T5U5pMrH3InX",
                    database="totesys",
                    port="5432",
                )

    sql = """
            SELECT
                table_name
            FROM
                information_schema.tables
            WHERE table_schema='public' AND table_name ~ '^[a-z]'
        """
    return connection.run(sql)