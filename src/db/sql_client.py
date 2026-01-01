import psycopg2 as pg
from pandas import DataFrame
from src.utils.logger import logger
from src.utils.constants import (
    DB_NAME, DB_USER, DB_PASSWORD, 
    DB_HOST, DB_PORT
)

class SQLClient:
    def __init__(self):
        self.conn = pg.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

    def store_neo_data(self, neo_df: DataFrame):
        self._create_neo_table()

        for _, row in neo_df.iterrows():
            # make sure to upsert data to avoid duplicates
            query = """
                INSERT INTO near_earth_objects (
                    reference_id, neo_name, nasa_jpl_url,
                    absolute_magnitude_h, estimated_diameter_min_km,
                    estimated_diameter_max_km, is_potentially_hazardous,
                    is_sentry_object
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (reference_id) 
                DO UPDATE SET
                    neo_name = EXCLUDED.neo_name,
                    nasa_jpl_url = EXCLUDED.nasa_jpl_url,
                    absolute_magnitude_h = EXCLUDED.absolute_magnitude_h,
                    estimated_diameter_min_km = EXCLUDED.estimated_diameter_min_km,
                    estimated_diameter_max_km = EXCLUDED.estimated_diameter_max_km,
                    is_potentially_hazardous = EXCLUDED.is_potentially_hazardous,
                    is_sentry_object = EXCLUDED.is_sentry_object;
            """
            params = (
                row["reference_id"], row["clean_neo_name"], row["nasa_jpl_url"],
                row["absolute_magnitude_h"], row["estimated_diameter_min_km"],
                row["estimated_diameter_max_km"], row["is_potentially_hazardous"],
                row["is_sentry_object"]
            )

            self._execute_query(query, params)
            self.conn.commit()

            logger.info(f"Updated data stored in database for NEO: {row['reference_id']}")

    def store_approach_data(self, approach_df: DataFrame):
        self._create_orbiting_body_table()
        self._create_approach_table()

        for _, row in approach_df.iterrows():
            # try inserting the orbiting body
            query = """
                INSERT INTO orbiting_bodies (body)
                VALUES (%s)
                ON CONFLICT (body)
                DO NOTHING;
            """

            self._execute_query(query, (row["orbiting_body"],))
            self.conn.commit()

            # get the id of the orbiting body
            query = """
                SELECT id
                FROM orbiting_bodies
                WHERE body = %s
            """
            result = self._execute_query(query, (row["orbiting_body"],))
            if not result or len(result) > 1:
                logger.error("Failed to get orbiting body ID.")
                continue
            
            body_id = result[0][0]

            query = """
                INSERT INTO close_approaches (
                    reference_id, close_approach_date_epoch,
                    relative_velocity_kms, miss_distance_km,
                    orbiting_body
                ) VALUES (%s, %s, %s, %s, %s);
            """
            params = (
                row["reference_id"], row["close_approach_date_epoch"],
                row["relative_velocity_kms"], row["miss_distance_km"], body_id
            )

            self._execute_query(query, params)
            self.conn.commit()

            logger.debug(f"Added a close approach data for NEO: {row['reference_id']}")

    def close(self):
        self.conn.close()

    def _execute_query(self, query: str, params=None):
        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            try:
                result = cursor.fetchall()
                return result
            except pg.ProgrammingError:
                return None
            
    def _create_neo_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS near_earth_objects (
                reference_id INT PRIMARY KEY,
                neo_name VARCHAR,
                nasa_jpl_url VARCHAR,
                absolute_magnitude_h FLOAT,
                estimated_diameter_min_km FLOAT,
                estimated_diameter_max_km FLOAT,
                is_potentially_hazardous BOOLEAN,
                is_sentry_object BOOLEAN
            );
        """
        self._execute_query(query)
        self.conn.commit()

    def _create_approach_table(self):
        query = """
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            CREATE TABLE IF NOT EXISTS close_approaches (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                reference_id INT,
                close_approach_date_epoch BIGINT,
                relative_velocity_kms FLOAT,
                miss_distance_km FLOAT,
                orbiting_body INT,
                FOREIGN KEY (reference_id) REFERENCES near_earth_objects(reference_id),
                FOREIGN KEY (orbiting_body) REFERENCES orbiting_bodies(id)
            );
        """
        self._execute_query(query)
        self.conn.commit()

    def _create_orbiting_body_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS orbiting_bodies (
                id SERIAL PRIMARY KEY,
                body VARCHAR UNIQUE
            )

        """

        self._execute_query(query)
        self.conn.commit()