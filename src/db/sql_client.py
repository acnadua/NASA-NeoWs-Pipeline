import psycopg2 as pg
from pandas import DataFrame
from src.utils.logger import logger
from src.utils.constants import (
    DB_NAME, DB_USER, DB_PASSWORD, 
    DB_HOST, DB_PORT, DB_SCHEMA
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

        self.cursor = self.conn.cursor()
        self._initialize()

    def _initialize(self):
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DB_SCHEMA}")
        self.conn.commit()

        self.cursor.execute(f"SET search_path TO {DB_SCHEMA}")

        # create tables
        self._create_neo_table()
        self._create_orbiting_body_table()
        self._create_approach_table()

        # create indices
        self._create_indices()

    def _create_indices(self):
        commands = [
            # foreign key indices
            "CREATE INDEX IF NOT EXISTS idx_approaches_reference_id ON close_approaches (reference_id);",
            "CREATE INDEX IF NOT EXISTS idx_approaches_orbiting_body ON close_approaches (orbiting_body);",
            # time series optimization
            "CREATE INDEX IF NOT EXISTS idx_approaches_date ON close_approaches (close_approach_date_stamp DESC);",
            # optimization for searching high-threat objects
            "CREATE INDEX IF NOT EXISTS idx_hazardous_neos ON near_earth_objects (reference_id) WHERE is_potentially_hazardous IS TRUE;",
            # optimization for querying closest approaches of the year
            "CREATE INDEX IF NOT EXISTS idx_approaches_date_distance ON close_approaches (close_approach_date_stamp, miss_distance_km);"
        ]

        for command in commands:
            self.cursor.execute(command)

        self.conn.commit()

    def store_neo_data(self, neo_df: DataFrame):
        for _, row in neo_df.iterrows():
            # make sure to upsert data to avoid duplicates
            command = """
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

            self.cursor.execute(command, params)
            self.conn.commit()

            logger.info(f"Updated data stored in database for NEO: {row['reference_id']}")

    def store_approach_data(self, approach_df: DataFrame):
        for _, row in approach_df.iterrows():
            # try inserting the orbiting body
            command = """
                INSERT INTO orbiting_bodies (body)
                VALUES (%s)
                ON CONFLICT (body)
                DO NOTHING;
            """

            self.cursor.execute(command, (row["orbiting_body"],))
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

            command = """
                INSERT INTO close_approaches (
                    reference_id,
                    close_approach_date_stamp,
                    relative_velocity_kms, miss_distance_km,
                    orbiting_body
                ) VALUES (%s, TO_TIMESTAMP(%s / 1000), %s, %s, %s);
            """
            params = (
                row["reference_id"], row["close_approach_date_epoch"],
                row["relative_velocity_kms"], row["miss_distance_km"], body_id
            )

            self.cursor.execute(command, params)
            self.conn.commit()

            logger.debug(f"Added a close approach data for NEO: {row['reference_id']}")

    def close(self):
        self.conn.close()

    def _execute_query(self, query: str, params=None):
        self.cursor.execute(query, params)
        try:
            result = self.cursor.fetchall()
            return result
        except pg.ProgrammingError:
            return None
            
    def _create_neo_table(self):
        command = """
            CREATE TABLE IF NOT EXISTS near_earth_objects (
                reference_id BIGINT PRIMARY KEY,
                neo_name VARCHAR,
                nasa_jpl_url VARCHAR,
                absolute_magnitude_h NUMERIC,
                estimated_diameter_min_km NUMERIC,
                estimated_diameter_max_km NUMERIC,
                is_potentially_hazardous BOOLEAN,
                is_sentry_object BOOLEAN
            );
        """
        self.cursor.execute(command)
        self.conn.commit()

    def _create_approach_table(self):
        command = """
            CREATE TABLE IF NOT EXISTS close_approaches (
                id SERIAL PRIMARY KEY,
                reference_id BIGINT,
                close_approach_date_stamp TIMESTAMPTZ,
                relative_velocity_kms NUMERIC,
                miss_distance_km NUMERIC,
                orbiting_body INT,
                FOREIGN KEY (reference_id) REFERENCES near_earth_objects(reference_id),
                FOREIGN KEY (orbiting_body) REFERENCES orbiting_bodies(id)
            );
        """
        self.cursor.execute(command)
        self.conn.commit()

    def _create_orbiting_body_table(self):
        command = """
            CREATE TABLE IF NOT EXISTS orbiting_bodies (
                id SERIAL PRIMARY KEY,
                body VARCHAR UNIQUE
            )
        """

        self.cursor.execute(command)
        self.conn.commit()