import boto3

def lambda_handler(event, context):
    client = boto3.client('appflow')
    flow_ids = event['flow_ids']
    started_flows = []
    for flow_id in flow_ids:
        try:
            client.start_flow(flowName=flow_id)
            started_flows.append({"flow_id": flow_id, "status": "Started"})
        except Exception as e:
            started_flows.append({"flow_id": flow_id, "status": "Failed", "error": str(e)})
    return {"started_flows": started_flows}
