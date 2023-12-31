import json
import requests

def lambda_handler(event, context):
    headers = {
    'Content-Type': 'application/json',
    'accept': 'application/json'
    }
    
    
    s3_event = event['Records'][0]['s3']

    # Extract relevant information from the event data
    bucket_name = s3_event['bucket']['name']
    object_key = s3_event['object']['key']
    s3_path = str(bucket_name)+str(object_key)
    
    data = json.dumps({
        'conf': {
            
            'data': s3_path
        }
        
    })
    
    
    # Perform further processing with the bucket and object information
    # For example, you can download the object or access its metadata

    # Print the extracted information
    print(f"Bucket: {bucket_name}")
    print(f"Object Key: {object_key}")
    
    # response = requests.post('http://ec2-3-229-229-155.compute-1.amazonaws.com:8080/api/v1/dags/tutorial_taskflow_api/dagRuns', headers=headers, data=json.dumps(data))

    # print(response.text)  # Print the content of the response
    
    endpoint= 'your-airflow-endpoint'
    
    username = 'username'
    password = 'password'
    
    response = requests.post(endpoint, data=data, headers=headers, auth=(username, password))
    print(response.status_code)


    # Check the response status code
    # if response.status_code == 200:
    #     print('POST request successful')
    # else:
    #     print('POST request failed')

    