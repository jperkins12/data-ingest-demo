import sys

import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
from sqlalchemy.sql import text


class KaggleDataGetter(object):
    def __init__(self) -> None:
        self.api = KaggleApi()
        self.api.authenticate()

    # Download dataset from Kaggle
    def downloadDataFile(
        self, dataset: str, file: str, output_path: str = "tmp"
    ) -> None:
        print(f"Downloading: {file}")
        self.api.dataset_download_file(
            dataset=dataset, file_name=file, path=output_path
        )
        print(f"Downloaded")

    def __checkLineCount(self, file: str) -> int:
        with open(file, "r") as f:
            line_count = len(f.readlines())
        return line_count

    # Fetch dataset from Kaggle and return as pandas dataframe
    def getAsDataframe(self, dataset: str, file: str) -> pd.DataFrame:
        self.downloadDataFile(dataset, file)
        tmp_path = f"tmp/{file}"
        print(f"Lines in file: {self.__checkLineCount(tmp_path)}")
        df = pd.read_csv(tmp_path)
        print(f"Lines in DF: {df.shape[0]}")
        return df


class PgExporter(object):
    def __init__(self, connection_str: str) -> None:
        self.engine = create_engine(connection_str)

    def __checkSchema(self, pg_schema: str) -> None:
        print(f"Checking Schema: {pg_schema}")
        with self.engine.connect() as con:
            if not self.engine.dialect.has_schema(con, pg_schema):
                print("Schema does not exist")
                con.execute(CreateSchema(pg_schema))
                con.commit()

    def __checkRowCount(self, table: str, schema: str) -> int:
        # need to make sqlsafe
        with self.engine.connect() as con:
            print(f"Checking row count for {schema}.{table}")
            results = con.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
            row_count = results.first()[0]
            assert isinstance(row_count, int)
            print(f"Found {row_count} rows")
        return row_count

    def exportToPG(self, df: pd.DataFrame, table_name: str, pg_schema) -> None:
        self.__checkSchema(pg_schema)
        print(f"Exporting to {pg_schema}.{table_name}")
        df.to_sql(
            name=table_name, con=self.engine, schema=pg_schema, if_exists="replace"
        )
        pgRowCount = self.__checkRowCount(table_name, pg_schema)
        if df.shape[0] != pgRowCount:
            print("ERROR: Not all rows exported!")


def main():
    CENSUS_DATASET_NAME = "muonneutrino/us-census-demographic-data"
    CENSUS_FILE_NAME = "acs2017_county_data.csv"

    COVID_DATASET_NAME = "nightranger77/covid19-state-data"
    COVID_FILE_NAME = "COVID19_state.csv"

    # If this wasn't a demo these would be stored in a .env file
    PG_HOST = "localhost"
    PG_PORT = 5432
    PG_USER = "postgres"
    PG_PASSWORD = "test1234"
    PG_DB = "postgres"
    PG_SCHEMA = "data_demo"

    # Get data in pandas
    dataGetter = KaggleDataGetter()
    censusDf = dataGetter.getAsDataframe(CENSUS_DATASET_NAME, CENSUS_FILE_NAME)
    covidDf = dataGetter.getAsDataframe(COVID_DATASET_NAME, COVID_FILE_NAME)

    # export to PG
    connection_string = (
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )

    pgExporter = PgExporter(connection_string)
    pgExporter.exportToPG(
        censusDf, CENSUS_FILE_NAME.replace(".csv", "").lower(), PG_SCHEMA
    )
    pgExporter.exportToPG(
        covidDf, COVID_FILE_NAME.replace(".csv", "").lower(), PG_SCHEMA
    )

    print("Done!")


if __name__ == "__main__":
    sys.exit(main())
