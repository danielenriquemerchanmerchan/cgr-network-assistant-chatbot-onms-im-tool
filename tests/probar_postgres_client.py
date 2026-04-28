"""
probar_postgres_client.py
-------------------------
Test rapido del cliente Postgres.
"""

from integrations.postgres.client import (
    obtener_conexion,
    cerrar_conexion,
    contar_filas,
    obtener_wonums_activos,
)


def main():
    print("="*60)
    print("PRUEBA DEL CLIENTE POSTGRES")
    print("="*60)

    # 1. Abrir conexion
    conn = obtener_conexion()
    if conn is None:
        print("[ERROR] No se pudo abrir conexion")
        return

    try:
        # 2. Estadisticas
        print("\nConteo de filas:")
        stats = contar_filas(conn)
        for tabla, count in stats.items():
            print(f"  {tabla:<25} {count:>6}")

        # 3. Wonums activos (deberia ser vacio porque no hay datos)
        wonums = obtener_wonums_activos(conn)
        print(f"\nWonums activos: {len(wonums)}")

    finally:
        cerrar_conexion(conn)

    print("\n[OK] Cliente funciona correctamente")


if __name__ == "__main__":
    main()