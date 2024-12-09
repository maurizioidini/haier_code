import sys
import boto3
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import logging
import time
import sys
from awsglue.utils import getResolvedOptions
import json

def get_logger():

    formatter = logging.Formatter(
        f'%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.setLevel(logging.INFO)

    return logger

def calculate_interval_date(interval_date):

    if GRANULARITY == 'day':
        new_interval_date = interval_date + timedelta(days=DELTA_GRANULARITY)
    if GRANULARITY == 'month':
        new_interval_date = interval_date + relativedelta(months=DELTA_GRANULARITY)
        
    if(new_interval_date > END_DATE):
        new_interval_date = END_DATE

    return new_interval_date

args = getResolvedOptions(sys.argv, ['flow_name', 'end_date', 'granularity', 'delta_granularity', 'field_to_filter'])
FLOW_NAME = args['flow_name']
END_DATE = datetime.strptime(args['end_date'], "%Y-%m-%d")
GRANULARITY = args['granularity']
DELTA_GRANULARITY = int(args['delta_granularity'])
FIELD_TO_FILTER = args['field_to_filter']

if GRANULARITY not in ['day', 'month']:
    raise Exception("Granularity has to be 'day' or 'month'")
if DELTA_GRANULARITY <= 0:
    raise Exception("Delta Granularity has to be > 0")

logger = get_logger()
appflow_client = boto3.client('appflow')
ssm = boto3.client("ssm")
initial_load_status = json.loads(ssm.get_parameter(Name = f'/initial-load/{FLOW_NAME}')['Parameter']['Value'])
previous_interval_start_date = datetime.strptime(initial_load_status['interval_start_date'], "%Y-%m-%d")
previous_interval_end_date = datetime.strptime(initial_load_status['interval_end_date'], "%Y-%m-%d")

flow_details = appflow_client.describe_flow(flowName=FLOW_NAME)

if flow_details['flowStatus'] != 'Active':
    raise Exception("Automated initial load is possible only for Active Flows. Please configure your flow as an Active flow")

if flow_details['triggerConfig']['triggerType'] != 'OnDemand':
    raise Exception("Automated initial load is possible only for OnDemand Flows. Please configure your flow as an OnDemand flow")

original_tasks = flow_details['tasks']

original_tasks = [task for task in original_tasks if task['connectorOperator']['SAPOData'] not in ['GREATER_THAN_OR_EQUAL_TO', 'LESS_THAN']]

latest_execution = appflow_client.describe_flow_execution_records(flowName=FLOW_NAME, maxResults=1)['flowExecutions'][0]

if latest_execution['executionStatus'] == 'Successful':
    
    logger.info(f'ExecutionID {latest_execution["executionId"]} processed interval: {previous_interval_start_date}  -  {previous_interval_end_date}')
    interval_start_date = calculate_interval_date(previous_interval_start_date)
    interval_end_date = calculate_interval_date(interval_start_date)
    
    if interval_start_date < END_DATE:
        
        interval_start_date_str = interval_start_date.strftime("%Y-%m-%d")
        interval_end_date_str = (interval_end_date - timedelta(days=1)).strftime("%Y-%m-%d")
        
        process_start_date_epoch_millisecond = str(int(interval_start_date.timestamp() * 1000))
        process_end_date_epoch_millisecond = str(int(interval_end_date.timestamp() * 1000))
        
        range_tasks = [
            {
                'taskType': 'Filter',
                'sourceFields': [FIELD_TO_FILTER],  
                'connectorOperator': {
                    'SAPOData': 'GREATER_THAN_OR_EQUAL_TO'
                },
                'taskProperties': {
                    'VALUE': process_start_date_epoch_millisecond
                }       
            },
            {
                'taskType': 'Filter',
                'sourceFields': [FIELD_TO_FILTER],  
                'connectorOperator': {
                    'SAPOData': 'LESS_THAN'
                },
                'taskProperties': {
                    'VALUE': process_end_date_epoch_millisecond
                }
                
            }
        ]
        
        new_tasks = original_tasks + range_tasks
    
        appflow_client.update_flow(
            flowName=FLOW_NAME,
            sourceFlowConfig=flow_details['sourceFlowConfig'],
            triggerConfig={'triggerType': 'OnDemand'},
            destinationFlowConfigList=flow_details['destinationFlowConfigList'],
            tasks=new_tasks
        )
        
        new_value = {
            "interval_start_date": interval_start_date_str,
            "interval_end_date": interval_end_date_str
        }
        
        ssm.put_parameter(
            Name=f'/initial-load/{FLOW_NAME}',
            Value=json.dumps(new_value),
            Type='String',
            Overwrite=True,
        )
        
        appflow_client.start_flow(flowName=FLOW_NAME)
        
    else:
    
        logger.info('Initial Load Completed')

else:
    
    raise Exception(f'Flow last status is {latest_execution["executionStatus"]}. Inital load has to be restarted from {previous_interval_start_date}')
