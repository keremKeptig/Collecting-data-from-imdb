import json
import boto3
import botocore
from json import JSONEncoder
from time import sleep

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=15000, connect_timeout=15000, region_name="us-east-1" )
client = boto3.client('lambda', config=cfg, region_name="us-east-1")

def lambda_handler(event, context):
    
    
    for i in range(0, 19):
        
        if i == 18:
            inputForInvoker = {'Starting':str(i*3000), 'EndingPoint':str(56570), 'ObjectName':'new_list_' + str(i),'Current':'current_list' + str(i)}
        else:
            inputForInvoker = {'Starting':str(i*3000), 'EndingPoint':str((i+1)*3000), 'ObjectName':'new_list_' + str(i),'Current':'current_list' + str(i)}
        
        sleep(1)
        response = client.invoke(
            FunctionName = 'arn:aws:lambda:us-east-1:198273291209:function:web_scrap',
            InvocationType = 'Event',#event RequestResponse
            Payload = json.dumps(inputForInvoker, cls=NumpyArrayEncoder)
            )
    
        
