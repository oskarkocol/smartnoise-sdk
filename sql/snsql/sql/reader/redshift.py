import os

from .base import SqlReader, NameCompare, Serializer
from .engine import Engine


class RedshiftReader(SqlReader):
    """
        A dumb pipe that gets a rowset back from a database using
        a SQL string, and converts types to some useful subset
    """

    ENGINE = Engine.REDSHIFT

    def __init__(self, host=None, database=None, user=None, password=None, conn=None, **kwargs):
        DB_NAME = '######'
        # CLUSTER_IDENTIFIER = '######-redshift'
        # DB_USER = '#####'
        super().__init__(self.ENGINE)

        self.conn = None
        if conn is not None:
            self.conn = conn
        else:
            self.host = host
            self.database = database
            self.user = user
            if password is None:
                if "REDSHIFT_PASSWORD" in os.environ:
                    password = os.environ["REDSHIFT_PASSWORD"]
            self.password = password

            try:
                import redshift_connector
                self.api = redshift_connector
            except:
                pass

    def execute(self, query, *ignore, accuracy:bool=False):
        #cursor.execute("select * from book")
        #result: tuple = cursor.fetchall() #result: pandas.DataFrame = cursor.fetch_dataframe()
        if not isinstance(query, str):
            raise ValueError("Please pass strings to execute.  To execute ASTs, use execute_typed.")
        cnxn = self.conn
        if cnxn is None:
            # conn = self.api.redshift_connector.connect(
            #     host=self.host,
            #     database=self.database,
            #     user=self.user,
            #     password=self.password
            # )
            conn = self.api.redshift_connector.connect(
                iam=True,
                database=self.database,
                db_user=self.user,
                password='',
                user='',
                cluster_identifier=self.cluster_identifier,
                access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                #session_token="my_aws_session_token",
                region=os.environ["AWS_REGION"]
            )
            cnxn = conn.cursor()

        result = cnxn.execute(str(query)).fetch_dataframe()
        if result.total_rows == 0:
            return []
        else:
            df = result.to_dataframe()
            col_names = [tuple(df.columns)]
            rows = [tuple(row) for row in df.values]
            return col_names + rows

class RedshiftNameCompare(NameCompare):
    def __init__(self, search_path=None):
        self.search_path = search_path if search_path is not None else ["public"]

    def schema_match(self, query, meta):
        if query.strip() == "" and meta in self.search_path:
            return True
        return self.identifier_match(query, meta)

    def identifier_match(self, query, meta):
        query = self.clean_escape(query)
        meta = self.clean_escape(meta)
        if query == meta:
            return True
        if self.is_escaped(meta) and meta.lower() == meta:
            meta = meta.lower().replace('"', "")
        #if self.is_escaped(query) and query.lower() == query:
        if query.lower() == query:
            query = query.lower().replace('"', "") # TODO: Replace Single quote replacement to backtick `
        return meta == query

    def strip_escapes(self, value):
        return value.replace('"', "").replace("`", "").replace("[", "").replace("]", "")

    def should_escape(self, identifier):
        if self.is_escaped(identifier):
            return False
        if identifier.lower() in self.reserved():
            return True
        if identifier.replace(" ", "") == identifier.lower():
            return False
        else:
            return True

class RedshiftSerializer(Serializer):
    def __init__(self):
        super().__init__()

