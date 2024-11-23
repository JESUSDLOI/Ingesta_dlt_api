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

    url_template = url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol=IBM&apikey=FAIS3WI31KJGTRUX'
    
    while True:
        url = url_template.format()
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            break
        
        yield data
 

pipeline = dlt.pipeline(
    pipeline_name='inflation',
    destination='snowflake',
    dataset_name='inlflation_dataset',
)
# The response contains a list of issues
load_info = pipeline.run(get_businesses)

print(load_info)

