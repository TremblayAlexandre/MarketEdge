import json
from datetime import datetime
import importlib
import sys

AVAILABLE_HANDLERS = ['analyse', 'enhance', 'lookup', 'decision']


def lambda_handler(event, context):
    """
    Generic router that handles all requests.
    
    Detects event type and routes appropriately:
    - SQS events → forward directly to analyse handler
    - HTTP requests → route by path (/api/analyse, /api/enhance, etc.)
    - Status requests → /api/status/{job_id} for polling results
    """
    
    print(f"DEBUG - Full event: {json.dumps(event)}")
    
    # Check if this is an SQS event
    if 'Records' in event and event['Records']:
        # This is an SQS message - forward directly to analyse handler
        print(f"DEBUG - SQS event detected, forwarding to analyse handler")
        try:
            from analyse import lambda_handler as analyse_handler
            return analyse_handler(event, context)
        except Exception as e:
            print(f"ERROR - Failed to handle SQS event: {e}")
            import traceback
            traceback.print_exc()
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'SQS event handler error',
                    'message': str(e)
                })
            }
    
    # Extract path and method (HTTP request)
    path = event.get('path', event.get('rawPath', '/'))
    method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method', 'GET'))
    body = event.get('body', '{}')
    
    # Parse body if it's a string
    if isinstance(body, str):
        try:
            body_data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            body_data = {}
    else:
        body_data = body or {}
    
    print(f"DEBUG - Path: {path}, Method: {method}")
    
    # Extract action from path
    # Path looks like: /prod/api/analyse, /api/enhance, /api/status/job-123, etc.
    path_parts = path.strip('/').split('/')
    action = None
    path_param = None
    
    for i, part in enumerate(path_parts):
        if part and part != 'prod' and part != 'api':
            action = part
            # Check if there's a path parameter (e.g., job_id after /status/)
            if i + 1 < len(path_parts):
                path_param = path_parts[i + 1]
            break
    
    print(f"DEBUG - Action: {action}, Path param: {path_param}")
    
    # Handle /health
    if action == 'health' or path.endswith('/health'):
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'availableActions': AVAILABLE_HANDLERS,
                'usage': 'POST /api/{action} with appropriate body parameters'
            })
        }
    
    # Handle /api/status/{job_id} for polling results
    if action == 'status' and path_param:
        print(f"DEBUG - Status check for job: {path_param}")
        try:
            from analyse import get_job_status
            status_response = get_job_status(path_param)
            return status_response
        except ImportError:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Handler not found',
                    'message': 'Could not import analyse handler'
                })
            }
        except Exception as e:
            print(f"ERROR - Status check error: {e}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'error': 'Status check error',
                    'message': str(e)
                })
            }
    
    # Route to appropriate handler
    if action not in AVAILABLE_HANDLERS:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Invalid action',
                'requested': action,
                'availableActions': AVAILABLE_HANDLERS,
                'usage': f'POST /api/{{action}} where action is one of: {", ".join(AVAILABLE_HANDLERS)}'
            })
        }
    
    try:
        # Import the handler module
        print(f"DEBUG - Importing handler: {action}")
        handler_module = importlib.import_module(action)
        
        # Get the lambda_handler function
        handler_func = getattr(handler_module, 'lambda_handler')
        
        # Call the handler
        print(f"DEBUG - Calling {action}.lambda_handler()")
        
        handler_event = {
            **body_data,  # Include body parameters
            'path': path,
            'action': action,
            'method': method,
            'httpMethod': method
        }
        
        response = handler_func(handler_event, context)
        
        print(f"DEBUG - Handler returned status: {response.get('statusCode')}")
        
        return response
        
    except ImportError as e:
        print(f"ERROR - Failed to import {action}: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Handler not found',
                'action': action,
                'message': f'Could not import {action} handler',
                'details': str(e)
            })
        }
    except AttributeError as e:
        print(f"ERROR - {action}.py missing lambda_handler: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Handler missing function',
                'action': action,
                'message': f'{action}.py must have lambda_handler function',
                'details': str(e)
            })
        }
    except Exception as e:
        print(f"ERROR - Exception in {action} handler: {e}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Handler error',
                'action': action,
                'message': str(e),
                'traceback': traceback.format_exc()
            })
        }