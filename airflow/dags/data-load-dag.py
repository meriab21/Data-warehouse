def split_into_chunks(arr, n):
    return [arr[i: i + n] for i in range(0, len(arr), n)]


def read_data():
    import pandas as pd

    df = pd.read_csv(
        "/opt/airflow/data/20181024_d1_0830_0900.csv",
        skiprows=1,
        header=None,
        delimiter="\n",
    )

    series = df[0].str.split(";")

    pd_lines = []

    for line in series:
        old_line = [item.strip() for item in line]
        info_index = 4
        info = old_line[:info_index]
        remaining = old_line[info_index:-1]
        chunks = split_into_chunks(remaining, 6)
        for chunk in chunks:
            record = info + chunk
            pd_lines.append(record)

    new_df = pd.DataFrame(
        pd_lines,
        columns=[
            "track_id",
            "type",
            "traveled_d",
            "avg_speed",
            "lat",
            "lon",
            "speed",
            "lon_acc",
            "lat_acc",
            "time",
        ],
    )

    return new_df.shape


def insert_data():
    import pandas as pd
    from sqlalchemy.types import Integer, Numeric, String

    pg_hook = PostgresHook(postgres_conn_id=f"traffic_flow_{deployment}")
    conn = pg_hook.get_sqlalchemy_engine()
    df = pd.read_csv(
        "/opt/airflow/data/20181024_d1_0830_0900.csv",
        skiprows=1,
        header=None,
        delimiter="\n",
    )

    series = df[0].str.split(";")

    pd_lines = []

    for line in series:
        old_line = [item.strip() for item in line]
        info_index = 4
        info = old_line[:info_index]
        remaining = old_line[info_index:-1]
        chunks = split_into_chunks(remaining, 6)
        for chunk in chunks:
            record = info + chunk
            pd_lines.append(record)

    new_df = pd.DataFrame(
        pd_lines,
        columns=[
            "track_id",
            "type",
            "traveled_d",
            "avg_speed",
            "lat",
            "lon",
            "speed",
            "lon_acc",
            "lat_acc",
            "time",
        ],
    )

    new_df.to_sql(
        "traffic_flow",
        con=conn,
        if_exists="replace",
        index=True,
        index_label="id",
        dtype={
            "track_id": Integer(),
            "traveled_d": Numeric(),
            "avg_speed": Numeric(),
            "lat": Numeric(),
            "lon": Numeric(),
            "speed": Numeric(),
            "lon_acc": Numeric(),
            "lat_acc": Numeric(),
            "time": Numeric(),
        },
    )


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 7, 19, 8, 25, 00),
    "concurrency": 1,
    "retries": 0,
}


dag = DAG(
    "load_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)
envs = ["dev", "stg", "prod"]

with dag:
    start = DummyOperator(task_id="start")

    # read_data_op = PythonOperator(
    #     task_id="read_data", python_callable=read_data
    # )

    create_table_op = PostgresOperator(
        task_id=f"create_pg_table_{deployment}",
        postgres_conn_id=f"traffic_flow_{deployment}",
        sql="""
            create table if not exists traffic_flow (
                id serial,
                track_id integer,
                type text,
                traveled_d numeric,
                avg_speed numeric,
                lat numeric,
                lon numeric,
                speed numeric,
                lon_acc numeric,
                lat_acc numeric,
                time numeric
            )
        """,
    )

    load_data_op = PythonOperator(
        task_id=f"load_data_{deployment}", python_callable=insert_data
    )

    start >> create_table_op >> load_data_op
