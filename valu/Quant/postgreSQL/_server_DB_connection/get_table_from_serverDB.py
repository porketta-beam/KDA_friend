import psycopg2
from dotenv import load_dotenv
import os

def load_env(env_file):
    """지정한 .env 파일에서 환경 변수를 로드합니다."""
    load_dotenv(env_file, override=True)
    return {
        "user": os.getenv("user"),
        "password": os.getenv("password"),
        "host": os.getenv("host"),
        "port": int(os.getenv("port", 5432)),
        "dbname": os.getenv("dbname"),
        "sslmode": os.getenv("sslmode", "disable")
    }

def create_connection(env_config):
    """환경 변수로 DB 연결을 생성합니다."""
    conn_params = {
        "user": env_config["user"],
        "password": env_config["password"],
        "host": env_config["host"],
        "port": env_config["port"],
        "dbname": env_config["dbname"]
    }
    if env_config["sslmode"]:
        conn_params["sslmode"] = env_config["sslmode"]
    return psycopg2.connect(**conn_params)

def find_table(conn, table_name):
    """Supabase에서 테이블이 존재하는지 확인합니다."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s;
        """, (table_name,))
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except Exception as e:
        print(f"❌ 테이블 검색 실패: {e}")
        return False

def get_table_schema(conn, table_name):
    """지정한 테이블의 스키마를 가져옵니다."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name, udt_name, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = %s AND table_schema = 'public';
        """, (table_name,))
        schema = cursor.fetchall()
        
        cursor.execute("""
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = %s AND table_schema = 'public' AND constraint_type = 'PRIMARY KEY';
        """, (table_name,))
        pk_constraint = cursor.fetchone()
        pk_columns = []
        if pk_constraint:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.constraint_column_usage
                WHERE table_name = %s AND constraint_name = %s;
            """, (table_name, pk_constraint[0]))
            pk_columns = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        return schema, pk_columns
    except Exception as e:
        print(f"❌ 스키마 가져오기 실패: {e}")
        return None, None

def create_table_in_local(conn, table_name, schema, pk_columns):
    """로컬 DB에 테이블을 생성합니다."""
    try:
        cursor = conn.cursor()
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        columns = []
        for column in schema:
            col_name, udt_name, is_nullable, col_default = column
            if udt_name.startswith('_'):
                data_type = f"{udt_name[1:]}[]"
            else:
                data_type = udt_name
            col_def = f"{col_name} {data_type}"
            if col_default:
                col_def += f" DEFAULT {col_default}"
            if is_nullable == "NO":
                col_def += " NOT NULL"
            columns.append(col_def)
        
        if pk_columns:
            columns.append(f"PRIMARY KEY ({', '.join(pk_columns)})")
        
        create_sql += ", ".join(columns) + ");"
        print(f"Generated SQL: {create_sql}")
        cursor.execute(create_sql)
        conn.commit()
        print(f"✅ 테이블 '{table_name}'이 로컬 DB에 생성되었습니다.")
        cursor.close()
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")

def copy_table_data(supabase_conn, local_conn, table_name, schema):
    """Supabase에서 데이터를 가져와 로컬 DB에 삽입합니다."""
    try:
        # Supabase에서 데이터 조회
        supabase_cursor = supabase_conn.cursor()
        supabase_cursor.execute(f'SELECT * FROM "{table_name}";')
        rows = supabase_cursor.fetchall()
        column_names = [desc[0] for desc in supabase_cursor.description]
        supabase_cursor.close()

        if not rows:
            print(f"ℹ️ 테이블 '{table_name}'에 데이터가 없습니다.")
            return

        # 로컬 DB에 데이터 삽입
        local_cursor = local_conn.cursor()
        placeholders = ", ".join(["%s"] * len(column_names))
        insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders});"
        
        local_cursor.executemany(insert_sql, rows)
        local_conn.commit()
        print(f"✅ 테이블 '{table_name}'의 {len(rows)}개 행이 로컬 DB에 삽입되었습니다.")
        local_cursor.close()
    except Exception as e:
        print(f"❌ 데이터 복사 실패: {e}")

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    supabase_env_path = os.path.join(base_dir, '.env.supabase')
    local_env_path = os.path.join(base_dir, '.env.local')

    supabase_env = load_env(supabase_env_path)
    local_env = load_env(local_env_path)
    
    table_name = "Account_Processed_Data"
    
    supabase_conn = None
    local_conn = None
    try:
        supabase_conn = create_connection(supabase_env)
        print("✅ Supabase 연결 성공")
        
        if not find_table(supabase_conn, table_name):
            print(f"❌ Supabase에 테이블 '{table_name}'이 존재하지 않습니다.")
            return
        
        schema, pk_columns = get_table_schema(supabase_conn, table_name)
        if not schema:
            print(f"❌ 테이블 '{table_name}'의 스키마를 가져오지 못했습니다.")
            return
        
        local_conn = create_connection(local_env)
        print("✅ 로컬 DB 연결 성공")
        create_table_in_local(local_conn, table_name, schema, pk_columns)
        
        # 데이터 복사
        # copy_table_data(supabase_conn, local_conn, table_name, schema)
        
    except Exception as e:
        print(f"❌ 에러 발생: {e}")
    finally:
        if supabase_conn:
            supabase_conn.close()
            print("🔒 Supabase 연결 종료")
        if local_conn:
            local_conn.close()
            print("🔒 로컬 DB 연결 종료")

if __name__ == "__main__":
    main()