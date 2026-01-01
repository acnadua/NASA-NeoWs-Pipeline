import os
from pipeline import NeoWsPipeline # Import your class

def lambda_handler(event, context):    
    pipeline = NeoWsPipeline()
    pipeline.run()
    
    return {
        "statusCode": 200, 
        "body": "Pipeline executed successfully"
    }