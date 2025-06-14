from prefect import flow, task
from etl import extract, transform, load


@task
def extract_task():
    return extract()

@task
def transform_task(data):
    return transform(data)

@task
def load_task(data):
    return load(data)

@flow
def etl_flow():
    data = extract_task()
    transformed = transform_task(data)
    load_task(data)


if __name__ == "__main__":
    etl_flow()
    