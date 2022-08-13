# Necessário importar todas as bibliotecas necessárias
# Bibliotecas de conexão ao banco
import mysql.connector
from mysql.connector import Error
import pandas as pd

# Senha de conexão
pw = '@LEX@tcc2022'
db = 'quintoAndar'

# Funcao para estabelecer conexao com o servidor

def create_server_connection(host_name, user_name, user_password):
    # Fecha qualquer conexao ativa
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# Estabelecendo a chamada para conexão com o servidor
connection = create_server_connection("localhost", "root", pw)


# Criando uma nova Base de Dados no servidor
def create_database(connection, query):
    # cursor é o objeto que estabelece a conexão ao servidor. Possui diversos métodos atribuídos a si.
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


create_database_query = "CREATE DATABASE quintoAndar"
create_database(connection, create_database_query)

# Conectando à Base de Dados
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


# Criando uma função para executar queries
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def execute_query_mod(connection, query, args):
    cursor = connection.cursor()
    try:
        cursor.execute(query, args)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

create_integracao_ia_table = """
CREATE TABLE integracao_ia (
  id_solicitacao INT PRIMARY KEY,
  usuario_solicitacao VARCHAR(40) NOT NULL,
  horario_solicitacao TIMESTAMP,
  tipo_solicitacao INT
  );
 """

# connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
# execute_query(connection, create_integracao_ia_table) # Execute our defined query

pop_integracao_ia_table = """
INSERT INTO integracao_ia VALUES
(1,  'Pedro', STR_TO_DATE("07,02,2022 10,47,40","%d,%m,%Y %H,%i,%s"), 1),
(2,  'Rodrigo', STR_TO_DATE("08,02,2022 11,47,40","%d,%m,%Y %H,%i,%s"), 1), 
(3,  'Luciana', STR_TO_DATE("08,02,2022 12,47,40","%d,%m,%Y %H,%i,%s"), 1),
(4,  'Pedro', STR_TO_DATE("09,02,2022 13,47,40","%d,%m,%Y %H,%i,%s"), 1),
(5,  'Rodrigo', STR_TO_DATE("09,02,2022 14,47,40","%d,%m,%Y %H,%i,%s"), 1);
"""

# connection = create_db_connection("localhost", "root", pw, db)
# execute_query(connection, pop_integracao_ia_table)

create_identificacao_ia_table = """
CREATE TABLE identificacao_ia (
  id_identificacao INT PRIMARY KEY,
  verificacao_usuario_identificacao VARCHAR(1) NOT NULL,
  nome_usuario_identificacao VARCHAR(40) NOT NULL,
  acuracia_identificacao INT,
  horario_identificacao TIMESTAMP
  );
 """

# connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
# execute_query(connection, create_identificacao_ia_table) # Execute our defined query

pop_identificacao_ia_table = """
INSERT INTO identificacao_ia VALUES
(1,  'S', 'Leonardo', 88, STR_TO_DATE("08,02,2022 10,47,40","%d,%m,%Y %H,%i,%s")),
(2,  'S', 'Raphael', 90, STR_TO_DATE("08,02,2022 11,48,18","%d,%m,%Y %H,%i,%s")), 
(3,  'S', 'Donatello', 75, STR_TO_DATE("09,02,2022 12,48,28","%d,%m,%Y %H,%i,%s")),
(4,  'S', 'Michelangelo', 92, STR_TO_DATE("09,02,2022 13,49,38","%d,%m,%Y %H,%i,%s")),
(5,  'N', 'Não identificado', 42, STR_TO_DATE("09,02,2022 14,49,42","%d,%m,%Y %H,%i,%s")),
(6,  'S', 'Anderson', 90, STR_TO_DATE("16,02,2022 16,40,00","%d,%m,%Y %H,%i,%s")),
(7,  'S', 'Antonio', 85, STR_TO_DATE("17,02,2022 10,15,00","%d,%m,%Y %H,%i,%s")),
(8,  'S', 'Joana', 86, STR_TO_DATE("18,02,2022 10,15,00","%d,%m,%Y %H,%i,%s")),
(9,  'S', 'Andreza', 86, STR_TO_DATE("18,02,2022 22,10,00","%d,%m,%Y %H,%i,%s")),
(10,  'S', 'Andre', 85, STR_TO_DATE("19,02,2022 12,10,00","%d,%m,%Y %H,%i,%s")),
(11,  'N', 'Não identificado', 40, STR_TO_DATE("19,02,2022 13,15,00","%d,%m,%Y %H,%i,%s")),
(12,  'S', 'Joao', 89, STR_TO_DATE("20,02,2022 09,05,00","%d,%m,%Y %H,%i,%s")),
(13,  'S', 'Tavares', 91, STR_TO_DATE("20,02,2022 10,15,00","%d,%m,%Y %H,%i,%s")),
(14,  'S', 'Sidão', 95, STR_TO_DATE("20,02,2022 14,35,00","%d,%m,%Y %H,%i,%s"))
;
"""


#connection = create_db_connection("localhost", "root", pw, db)
#execute_query(connection, pop_identificacao_ia_table)

# Ler Query
def read_query(connection, query):
    cursor = connection.cursor(dictionary=True)
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as err:
        print(f"Error: '{err}'")


ler_identificacao_ia = """
SELECT id_identificacao, verificacao_usuario_identificacao, nome_usuario_identificacao, acuracia_identificacao, horario_identificacao
FROM identificacao_ia;
"""

ler_integracao_ia = """
SELECT id_solicitacao, usuario_solicitacao, horario_solicitacao
FROM tcc.integracao_ia;
"""

drop_identificacao_ia = """
ALTER TABLE identificacao_ia
DROP COLUMN horario_identificacao;
"""

alter_identificacao_ia = """
ALTER TABLE identificacao_ia
ADD COLUMN horario_identificacao DATE;
"""

ins_identificacao_ia_table = """
UPDATE identificacao_ia 
SET horario_identificacao = STR_TO_DATE('2022-02-08','%Y-%m-%d') 
WHERE verificacao_usuario_identificacao = 'S';
"""

drop_all_identificacao_ia = """
DROP TABLE identificacao_ia;
"""

# connection = create_db_connection("localhost", "root", pw, db)
# results = read_query(connection, ler_identificacao_ia)

def inserir_novos_usuarios_integracao(connection, id_solicitacao, usuario_solicitacao, horario_solicitacao):
    args = (id_solicitacao, usuario_solicitacao, horario_solicitacao, 1)
    ins_integracao_ia_table = "INSERT INTO integracao_ia " \
                              "VALUES (%s,%s,%s,%s)"

    execute_query_mod(connection, ins_integracao_ia_table, args)
    create_csv_integracao_ia(connection)
