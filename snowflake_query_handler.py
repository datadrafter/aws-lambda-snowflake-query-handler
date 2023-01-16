import snowflake.connector
 
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

class snowflake_query_handler():
    
    def __init__(self, snowflake_account, snowflake_warehouse, snowflake_user, snowflake_user_role, snowflake_user_key, snowflake_user_key_phrase):
        
        self.snowflake_account = snowflake_account
        self.snowflake_warehouse = snowflake_warehouse
        self.snowflake_user = snowflake_user
        self.snowflake_user_role = snowflake_user_role
        self.snowflake_user_key = snowflake_user_key.encode()
        self.snowflake_user_key_phrase = snowflake_user_key_phrase.encode()
        
    def connect(self): 
        p_key= serialization.load_pem_private_key(
            data=self.snowflake_user_key,
            password=self.snowflake_user_key_phrase,
            backend=default_backend()
        )

        pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

        ctx = snowflake.connector.connect(
            user= self.snowflake_user,
            account=self.snowflake_account,
            private_key=pkb,
            warehouse=self.snowflake_warehouse,
            role=self.snowflake_user_role
        )

        cs = ctx.cursor(snowflake.connector.DictCursor)
        
        return cs
        
    def execute_query(self, query): 
        
        con = self.connect()
        
        try:
            
            return con.execute(query).fetchall()
            
        except NameError:
            print(NameError)
            
        finally:
            con.close()