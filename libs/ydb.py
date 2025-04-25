import os
import ydb
import time
from libs.logs import logger
from typing import List, Dict, Any

try:
    driver = ydb.Driver(endpoint='grpcs://ydb.serverless.yandexcloud.net:2135', database=os.getenv('YDB_DATABASE'), credentials=ydb.credentials_from_env_variables())
    driver.wait(fail_fast=True, timeout=30)
    session_pool = driver.table_client.session().create()
except Exception as e: 
    logger.error(f"Ошибка при инициализации YDB драйвера: {e}") 
    raise e 


def upset_data(table: str, data: List[Dict[str, Any]]) -> bool:
    # Получаем имена колонок из первого словаря
    columns = list(data[0].keys())
    # Проверяем, что все словари имеют одинаковую структуру
    if not all(set(item.keys()) == set(columns) for item in data):
        raise ValueError("Все словари должны иметь одинаковую структуру")

    # Формируем блок DECLARE для параметров
    declare_block = []
    param_values = {}
    bind_vars_rows = []

    # Для каждой записи создаем набор параметров
    for row_idx, row in enumerate(data):
        bind_vars = []
        for col in columns:
            param_name = f"${col}_{row_idx}"
            bind_vars.append(param_name)
                
            # Определяем тип данных
            if col.upper() == "ID":
                param_values[param_name] = int(row[col])
            else:
                param_values[param_name] = str(row[col])

            # Добавляем DECLARE
            if col.upper() == "ID":
                declare_block.append(f"DECLARE {param_name} AS Int64;")
            else:
                declare_block.append(f"DECLARE {param_name} AS Utf8;")

        # Формируем строку значений для VALUES
        bind_vars_rows.append(f"({', '.join(bind_vars)})")

    # Формируем полный запрос
    query = f"""
    {' '.join(declare_block)}
    UPSERT INTO `{table}` ({', '.join(columns)})
    VALUES {', '.join(bind_vars_rows)};
    """

    # logger.info(f"{query}")
    # logger.info(param_values)

    # Подготавливаем и выполняем запрос
    prepared_query = session_pool.prepare(query)
    session_pool.transaction(ydb.SerializableReadWrite()).execute(
        prepared_query,
        param_values,
        commit_tx=True
    )
        
    return True
