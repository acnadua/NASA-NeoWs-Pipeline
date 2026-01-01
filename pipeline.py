import re
import pandas as pd
from src.db.sql_client import SQLClient
from src.db.aws_client import AWSClient
from src.client.api_client import fetch_neo_data
from src.transform.flatten_neo import extract_neo
from src.transform.clean_approaches import get_new_neo_approaches

class NeoWsPipeline:
    def __init__(self):
        self.sql_client = SQLClient()
        self.aws_client = AWSClient()

    def run(self):
        # extract data
        initial_data = fetch_neo_data()

        # store raw data in S3
        self.aws_client.save_data_to_s3(initial_data)

        # transform data
        flat_data = extract_neo(initial_data)
        neo, approaches = get_new_neo_approaches(flat_data)

        # create dataframes
        neo_df = pd.DataFrame(neo["neo"])
        approaches_df = pd.DataFrame(approaches)

        # standardize NEO names to a format: "YYYY <identifier>"
        regex = r"\d{4} [\w\d]+"
        neo_df["clean_neo_name"] = neo_df["neo_name"].apply(
          lambda name: (
            re.search(regex, name).group()  # type: ignore
            if re.search(regex, name) is not None 
            else name
          )
        )

        neo_df.insert(2, "clean_neo_name", neo_df.pop("clean_neo_name"))

        # store both datasets in Postgres
        self.sql_client.store_neo_data(neo_df)
        self.sql_client.store_approach_data(approaches_df)

        self.sql_client.close()