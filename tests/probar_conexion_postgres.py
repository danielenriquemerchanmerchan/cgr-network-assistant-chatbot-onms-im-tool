"""
probar_conexion_postgres.py
---------------------------
Prueba minima de conexion a Postgres.
Solo verifica que las credenciales del .env funcionen y que las
tablas del schema onms sean accesibles.

EJECUCION:
    py -m tests.probar_conexion_postgres
"""

import psycopg2
from core.config import PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE


def main():
    print("="*60)
    print("PRUEBA DE CONEXION A POSTGRES")
    print("="*60)
    print(f"Host:     {PG_HOST}")
    print(f"Port:     {PG_PORT}")
    print(f"Database: {PG_DATABASE}")
    print(f"User:     {PG_USER}")
    print(f"Password: {'***' if PG_PASSWORD else '(vacia!)'}")
    print()

    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE,
        )
        print("[OK] Conexion exitosa")

        cursor = conn.cursor()

        # Verificar que vemos las tablas del schema onms
        cursor.execute("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'onms' 
            ORDER BY tablename
        """)
        tablas = [row[0] for row in cursor.fetchall()]

        print(f"\nTablas en schema onms: {len(tablas)}")
        for t in tablas:
            print(f"  - {t}")

        # Verificar que podemos consultar work_orders
        cursor.execute("SELECT COUNT(*) FROM onms.work_orders")
        count_wo = cursor.fetchone()[0]
        print(f"\nFilas en onms.work_orders: {count_wo}")

        cursor.execute("SELECT COUNT(*) FROM onms.worklogs")
        count_wl = cursor.fetchone()[0]
        print(f"Filas en onms.worklogs: {count_wl}")

        cursor.execute("SELECT COUNT(*) FROM onms.bot_states")
        count_bs = cursor.fetchone()[0]
        print(f"Filas en onms.bot_states: {count_bs}")

        cursor.close()
        conn.close()

        print("\n[OK] Todo correcto. Estamos listos para construir el ETL.")

    except psycopg2.OperationalError as e:
        print(f"[ERROR] No se pudo conectar: {e}")
        print("\nVerifica:")
        print("  - PG_PASSWORD esta lleno en .env")
        print("  - PG_HOST y PG_PORT son correctos")
        print("  - Tu maquina puede alcanzar el servidor (ping/telnet)")
    except psycopg2.errors.UndefinedTable as e:
        print(f"[ERROR] Tabla no existe: {e}")
        print("Verifica que las tablas onms.work_orders, onms.worklogs, onms.bot_states existan")
    except Exception as e:
        print(f"[ERROR] Inesperado: {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()