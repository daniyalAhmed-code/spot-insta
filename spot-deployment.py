import json, requests, os, boto3, base64, logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SPOT_INST_URI = "https://api.spotinst.io/aws/ec2/group"
VICTOR_OPS_URI = "https://api.victorops.com/api-public/v1"
FORBIDDEN_DEPLOYMENT_STATUSES = ['failed', 'stopping']
SKIP_DEPLOYMENT_STATUSES = ['starting', 'in_progress']

class CustomError(Exception):
    pass

def lambda_handler(event, context):
    statusCode = 200
    message = "Couldn't start the deployment"
    do_skip_deployment = False
    
    try:
        spot_inst_secret_token = get_secret('SPOT_INST_AUTH_TOKEN')
        victor_ops_secret_token = json.loads(get_secret('VICTOR_OPS_AUTH_TOKEN'))
        BEARER_TOKEN = 'Bearer {}'.format(spot_inst_secret_token)
        grace_period = 600
        batch_size_percentage = 20
        
        if 'e_group_deployment_grace_period' in event:
            grace_period = event['e_group_deployment_grace_period']
            
        if 'e_group_deployment_batch_size_percentage' in event:
            batch_size_percentage = event['e_group_deployment_batch_size_percentage']
        
        e_group = get_elastic_group(event['e_group_name'], BEARER_TOKEN)
        
        if e_group == None:
            raise CustomError("Elastic group not found")
            
        e_group_deployment = get_elastic_group_current_deployment(e_group['id'], BEARER_TOKEN)
        
        if not e_group_deployment == None:
            message = "Deployment ({d_id}) is in '{d_status}' state".format(d_id=e_group_deployment['id'], d_status=e_group_deployment['status'])
            if e_group_deployment['status'] in FORBIDDEN_DEPLOYMENT_STATUSES:
                raise CustomError(message)
            elif e_group_deployment['status'] in SKIP_DEPLOYMENT_STATUSES:
                do_skip_deployment = True
        
        if not do_skip_deployment:
            deployment_response = perform_spot_inst_deployment(event, e_group, BEARER_TOKEN, grace_period, batch_size_percentage)
            if deployment_response['is_deployment_started']:
                message = deployment_response['message']

        acknowledgement_response = perform_victorops_incident_acknowledgement(event, victor_ops_secret_token)
        if acknowledgement_response['is_acknowledged']:
            message = message + acknowledgement_response['message']

    except CustomError as e:
        statusCode = 500
        message = str(e)
        logger.exception(e)
        
    except Exception as e:
        statusCode = 500
        message = "Internal server error"
        logger.exception(e)
    
    if 'send_to_slack' in event and event['send_to_slack']:
        send_to_slack(event, statusCode, message)
        
    
    return {
        'statusCode': statusCode,
        'message': message
    }
    
def get_e_group_deployment_configuration(e_group):
    batch_size_percentage = 10
    instance_target = e_group['capacity']['target']
    
    if instance_target >= 20:
        batch_size_percentage = 20
        
    return {
        "batch_size_percentage": batch_size_percentage,
        "instance_target" : instance_target
    }
    
def acknowledge_latest_victor_ops_incident(e_group_name, victor_ops_secret_token):
    is_success = False
    incident = None
    try:
        incident = get_latest_victor_ops_incident(e_group_name, victor_ops_secret_token)
        
        if not incident == None:
            logger.info(incident)
            ack_response = acknowledge_victor_ops_incident(incident['incidentNumber'], victor_ops_secret_token)
            if 'results' in ack_response and len(ack_response['results']) > 0 and 'cmdAccepted' in ack_response['results'][0]:
                is_success = ack_response['results'][0]['cmdAccepted']
    except Exception as e:
        logger.exception(e)
            
    return {
        "incident": incident,
        "is_success": is_success
    }
        
def acknowledge_victor_ops_incident(incident_number, victor_ops_secret_token):
    params = "/incidents/ack"
    headers = {
        'X-VO-Api-Id': victor_ops_secret_token['X-VO-Api-Id'],
        'X-VO-Api-Key':  victor_ops_secret_token['X-VO-Api-Key'],
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    data = {
      "userName": "umairhussain",
      "incidentNames": [
        incident_number
      ],
      "message": ""
    }
    r = requests.request('PATCH', (VICTOR_OPS_URI + params), headers=headers, data=json.dumps(data))
    r_json = json.loads(r.text)
    return r_json
    
def get_latest_victor_ops_incident(e_group_name, victor_ops_secret_token):
    formatted_e_group_name = e_group_name.lower()
    params = "/incidents"
    headers = {
        'X-VO-Api-Id': victor_ops_secret_token['X-VO-Api-Id'],
        'X-VO-Api-Key':  victor_ops_secret_token['X-VO-Api-Key'],
        'Accept': 'application/json'
    }
    r = requests.request('GET', (VICTOR_OPS_URI + params), headers=headers)
    r_json = json.loads(r.text)
    
    if 'incidents' in r_json and len(r_json['incidents']) > 0:
        for incident in r_json['incidents']:
            if incident['currentPhase'] == "UNACKED" and incident["routingKey"] == "routingexchange" and 'exchangename:' in incident['entityId']:
                split_result = incident['entityId'].split("exchangename:")
                if len(split_result) > 1 and split_result[1].lower() in formatted_e_group_name:
                    return incident

def get_elastic_group(name, BEARER_TOKEN):
    r = requests.request('GET', SPOT_INST_URI, headers={'Authorization': BEARER_TOKEN})
    r_json = json.loads(r.text)
    e_groups_list = []
    
    logger.info(r_json)
    
    if 'count' in r_json['response'] and r_json['response']['count'] > 0:
        e_groups_list.extend(r_json['response']['items'])
    
    for e_group in e_groups_list:
        if e_group['name'] == name:
            return e_group
            
def get_elastic_group_current_deployment(e_group_id, BEARER_TOKEN):
    params = "/{}/roll?limit=1&sort=createdAt:DESC".format(e_group_id)
    headers = {'Authorization': BEARER_TOKEN}
    r = requests.request('GET', (SPOT_INST_URI + params), headers=headers)
    r_json = json.loads(r.text)
    
    logger.info(r_json)
    
    if 'count' in r_json['response'] and r_json['response']['count'] > 0:
        return r_json['response']['items'][0]
        
def run_elastic_group_deployment(e_group_id, BEARER_TOKEN, grace_period, batch_size_percentage):
    params = "/{}/roll".format(e_group_id)
    headers = {
        'Authorization': BEARER_TOKEN,
        'Content-Type': 'application/json'
    }
    data = {
        'batchSizePercentage': batch_size_percentage,
        'gracePeriod': grace_period
    }
    
    r = requests.request('PUT', (SPOT_INST_URI + params), headers=headers, data=json.dumps(data))
    r_json = json.loads(r.text)
    
    logger.info(r_json)
    
    return r_json
    
def get_secret(secret_name):
    
    secret = None

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            
    return secret
    
    
def send_to_slack(event, status_code, message):
    try:
        e_group_name = "*" + event['e_group_name'] + "*"
        
        headers = {"Content-Type": "application/json"}
        message = "------------------ {} ------------------ \n".format(e_group_name) + message
        
        data = {
                "text": message,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": message
                        },
                        "block_id":"e_group_block",
                    }
                ]
            }
    
        r = requests.request(
            "POST", os.environ["SLACK_WEBHOOK"], headers=headers, data=json.dumps(data)
        )
        
    
        logger.info(r)
    
    except Exception as e:
        logger.exception(e)
    

def perform_spot_inst_deployment(event, e_group, BEARER_TOKEN, grace_period, batch_size_percentage):
    message = ""
    is_deployment_started = False
    e_group_info = get_e_group_deployment_configuration(e_group)
    e_group_run_deployment_response = run_elastic_group_deployment(e_group['id'], BEARER_TOKEN, grace_period, batch_size_percentage)

    if 'response' in e_group_run_deployment_response and e_group_run_deployment_response['response']['status']['code'] == 200 and len(e_group_run_deployment_response['response']['items']) > 0:
        is_deployment_started = True
        message = "Deployment ({d_id}) has been started. \nInstances: {instance_target} \nBatch size: {batch_size}% \nGrace period: {grace_period}s" \
                    .replace("{d_id}", str(e_group_run_deployment_response['response']['items'][0]['id'])) \
                    .replace("{instance_target}", str(e_group_info['instance_target'])) \
                    .replace("{grace_period}", str(grace_period))
                    
        if 'type' in event and event['type'] == "AUTOMATIC_DEPLOYMENT":
            batch_size_percentage = e_group_info['batch_size_percentage']
            message = message.replace("Deployment ", ":fire: Automatic deployment :fire: ")
        
        message = message.replace("{batch_size}", str(batch_size_percentage))
    else:
        raise CustomError("Couldn't start the deployment")

    return {
        "message": message,
        "is_deployment_started": is_deployment_started
    }

def perform_victorops_incident_acknowledgement(event, victor_ops_secret_token):
    message = ""
    is_acknowledged = False
    ack_response = acknowledge_latest_victor_ops_incident(event['e_group_name'], victor_ops_secret_token)
    
    if 'incident' in ack_response and not ack_response['incident'] == None:
        is_acknowledged = True
        message = "\nVictorOps Display Name: {}".format(ack_response['incident']['entityDisplayName'])
        message = message + "\nVictorOps incident {number} has been {is_success}".format(number=ack_response['incident']['incidentNumber'], is_success=("acknowledged" if ack_response['is_success'] else "not acknowledged"))

    return {
        "message": message,
        "is_acknowledged": is_acknowledged
    }