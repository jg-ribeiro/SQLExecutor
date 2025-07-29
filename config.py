from sqlalchemy import event, create_engine, text
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
from auxils import open_json
import datetime
import oracledb


class Config:
    # Carrega as configurações dos bancos
    _MAIN_PARAMETERS = open_json()
    _ORACLE = _MAIN_PARAMETERS['oracle_database']
    _POSTGRES = _MAIN_PARAMETERS['postgres']
    ARRAYSIZE = 15000

    # ============================================================================
    # ================================= INIT =====================================
    # ============================================================================

    def __init__(self) -> None:
        self._engine = self._get_postgres_engine()
        self._oracle_engine = self._get_oracle_engine()

    # ============================================================================
    # ============================== POSTGRESQL ==================================
    # ============================================================================

    def get_postgres_session(self):
        return sessionmaker(bind=self._engine)

    def _get_postgres_engine(self):
        """Cria e retorna um engine SQLAlchemy para o PostgreSQL."""
        database_url = self._get_postgres_url(self._POSTGRES)
        engine = create_engine(database_url)

        # Testa a conexão
        try:
            with engine.connect() as connection:
                result = connection.execute(text("SELECT version()"))
                print(f"Conectado com sucesso ao PostgreSQL: {result.fetchone()}")
            return engine
        except Exception as e:
            print(f"Erro ao conectar ao PostgreSQL: {e}")
            exit(1)

    def _get_postgres_url(self, pg_params):
        password_safe = quote_plus(pg_params['password'])
        return (
            f"postgresql+psycopg2://{pg_params['username']}:{password_safe}"
            f"@{pg_params['hostname']}:{pg_params['port']}/{pg_params['database']}"
        )

    # ============================================================================
    # ============================== ORACLEDB ====================================
    # ============================================================================

    def get_oracle_session(self):
        """Retorna um SessionMaker para o OracleDB."""
        return sessionmaker(bind=self._oracle_engine)

    def _get_oracle_engine(self):
        """
        Inicializa o Oracle client, cria e retorna um engine SQLAlchemy
        para o OracleDB usando TNS name configurado.
        """
        # 1) Inicializa o Oracle Instant Client
        try:
            oracledb.init_oracle_client(lib_dir=self._ORACLE['INSTANT_CLIENT'])
        except Exception as e:
            print(f"Erro ao inicializar Oracle client: {e}")
            exit(1)

        # 2) Recupera TSN (TNS name) definido pela empresa
        tsn = self._ORACLE['TSN']

        # 3) Monta a URL do SQLAlchemy: 
        #    note que o dialect é oracle+oracledb e o DSN é o TNS name
        user = self._ORACLE['user_name']
        pw   = quote_plus(self._ORACLE['user_pass'])
        url  = f"oracle+oracledb://{user}:{pw}@{tsn}"

        # 4) Cria engine e testa conexão
        engine = create_engine(url, arraysize=self.ARRAYSIZE)

        # —–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
        # injeta automaticamente o ALTER SESSION toda vez que uma conexão
        @event.listens_for(engine, "connect")
        def _set_nls_date_format(dbapi_connection, connection_record):
            # dbapi_connection é o objeto oracledb.Connection
            cursor = dbapi_connection.cursor()
            cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'DD/MM/YYYY'")
            cursor.close()
        # —–––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––


        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT SYSDATE FROM DUAL")).fetchone()
                assert result is not None, "Teste DQL retornou nenhuma linha"

                if not isinstance(result[0], datetime.datetime):
                    raise RuntimeError("Retorno inesperado no teste DQL")
                print(f"Conectado com sucesso ao OracleDB (TSN={tsn}): {result[0]}")
            return engine
        except Exception as e:
            print(f"Erro ao conectar ao OracleDB (TSN={tsn}): {e}")
            exit(1)


cfg = Config()

if __name__ == '__main__':
    OracleSession = cfg.get_oracle_session()

    with OracleSession() as session:
        # Executa um SQL text
        result = session.execute(text("SELECT CD_UPNIVEL1 FROM PIMSCS.HISTMANEJO WHERE DT_HISTORICO = '30/06/2025'"))
        
        # Itera sobre o ResultProxy
        for row in result:
            print(row)