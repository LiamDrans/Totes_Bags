DECLARE
    last_checked_time TIMESTAMP;

SELECT *
FROM where_ever
WHERE updated_time >= last_checked_time


cursor.execute("""
SELECT *
FROM where_ever
WHERE updated_time >= :last_checked_time""", (last_checked_time=last_checked_time))


2024-08-20 10:49:09.901


