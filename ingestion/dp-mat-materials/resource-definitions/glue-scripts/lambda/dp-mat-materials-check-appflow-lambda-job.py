import boto3

def lambda_handler(event, context):
    client = boto3.client('appflow')
    flow_ids = event['flow_ids']
    all_completed = True
    flow_statuses = {}
    for flow_id in flow_ids:
        response = client.describe_flow(flowName=flow_id)
        status = response['lastRunExecutionDetails']['mostRecentExecutionStatus']
        flow_statuses[flow_id] = status
        if status not in ['Succeeded', 'Failed']:
            all_completed = False
    return {"all_completed": all_completed, "flow_statuses": flow_statuses}
