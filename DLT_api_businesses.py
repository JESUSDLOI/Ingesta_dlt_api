import dlt
from dlt.sources.helpers import requests

@dlt.resource(
    table_name="businesses",
    merge_key="business_name",
    write_disposition={"disposition": "merge", "strategy": "scd2"}
    )

def get_businesses(
    updated_at=dlt.sources.incremental("updated_at", initial_value="2014-01-21T00:00:00Z")
):
    limit = 1000
    offset = 0
    url_template = "https://data.ny.gov/resource/n9v6-gdp6.json?$limit={limit}&$offset={offset}&$where=updated_at>'{updated_at}'"
    
    while True:
        url = url_template.format(limit=limit, offset=offset, updated_at=updated_at.last_value)
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            break
        
        yield data
        offset += limit

pipeline = dlt.pipeline(
    pipeline_name='businesses',
    destination='snowflake',
    dataset_name='businesses_dataset',
)
# The response contains a list of issues
load_info = pipeline.run(get_businesses)

print(load_info)

