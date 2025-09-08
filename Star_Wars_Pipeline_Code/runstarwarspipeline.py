import extract
import transform
import load

class RunStarWarsDataPipeline:


    print("Running Star Wars Data Pipeline...")
    data_json_list = extract.Extract().execute()
    data_transformed = transform.Transform().execute(data_json_list)
    load.load().execute(data_transformed)
    