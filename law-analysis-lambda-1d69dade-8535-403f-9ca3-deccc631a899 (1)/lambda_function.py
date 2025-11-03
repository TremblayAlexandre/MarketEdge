import json
from datetime import datetime
import importlib
import sys

AVAILABLE_HANDLERS = ['analyse', 'enhance', 'lookup', 'decision', 'chat']


def lambda_handler(event, context):
    """
    Generic router that handles all requests.
    
    For SQS events: checks job_type and routes to appropriate handler
    For HTTP requests: routes by path
    """
    
    print(f"DEBUG - Full event keys: {list(event.keys())}")
    
    # Check if this is an SQS event
    if 'Records' in event and isinstance(event.get('Records'), list) and len(event['Records']) > 0:
        print(f"DEBUG - SQS event detected with {len(event['Records'])} records")
        
        # Peek at first message to determine job type
        try:
            first_record = event['Records'][0]
            message_body = json.loads(first_record.get('body', '{}'))
            job_type = message_body.get('job_type', 'analyse')
            job_id = message_body.get('job_id', 'unknown')
            
            print(f"DEBUG - Job ID: {job_id}")
            print(f"DEBUG - Job type: {job_type}")
            
            if job_type == 'lookup':
                print(f"DEBUG - Routing SQS event to lookup handler")
                try:
                    from lookup import lambda_handler as lookup_handler
                    result = lookup_handler(event, context)
                    print(f"DEBUG - Lookup handler completed")
                    return result
                except Exception as e:
                    print(f"ERROR - Lookup handler failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': 'Lookup handler error', 'message': str(e)})
                    }
            else:
                print(f"DEBUG - Routing SQS event to analyse handler (job_type: {job_type})")
                try:
                    from analyse import lambda_handler as analyse_handler
                    result = analyse_handler(event, context)
                    print(f"DEBUG - Analyse handler completed")
                    return result
                except Exception as e:
                    print(f"ERROR - Analyse handler failed: {e}")
                    import traceback
                    traceback.print_exc()
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': 'Analyse handler error', 'message': str(e)})
                    }
        
        except Exception as e:
            print(f"ERROR - Could not parse SQS message: {e}")
            import traceback
            traceback.print_exc()
            # Fall back to analyse
            try:
                from analyse import lambda_handler as analyse_handler
                return analyse_handler(event, context)
            except Exception as e2:
                print(f"ERROR - Fallback to analyse failed: {e2}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'SQS routing error', 'message': str(e2)})
                }
    
    # ========================================================================
    # HTTP REQUEST HANDLING
    # ========================================================================
    
    # Extract path and method
    path = event.get('path', event.get('rawPath', '/'))
    method = event.get('httpMethod', event.get('requestContext', {}).get('http', {}).get('method', 'GET'))
    body = event.get('body', '{}')
    
    # Parse body
    if isinstance(body, str):
        try:
            body_data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            body_data = {}
    else:
        body_data = body or {}
    
    print(f"DEBUG - HTTP Path: {path}, Method: {method}")
    
    # Extract action from path
    path_parts = path.strip('/').split('/')
    action = None
    path_param = None
    
    for i, part in enumerate(path_parts):
        if part and part != 'prod' and part != 'api':
            action = part
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
                'availableActions': AVAILABLE_HANDLERS
            })
        }
    
    # Handle /api/status/{job_id} - check both analyse and lookup
    if action == 'status' and path_param:
        print(f"DEBUG - Status check for job: {path_param}")
        try:
            # Try lookup first
            try:
                from lookup import get_job_status as lookup_get_status
                status_response = lookup_get_status(path_param)
                status_code = status_response.get('statusCode', 500)
                if status_code != 404:
                    return status_response
            except Exception as e:
                print(f"DEBUG - Lookup status check skipped: {e}")
            
            # Fall back to analyse
            try:
                from analyse import get_job_status as analyse_get_status
                return analyse_get_status(path_param)
            except Exception as e:
                print(f"ERROR - Analyse status check failed: {e}")
                return {
                    'statusCode': 500,
                    'headers': {'Content-Type': 'application/json'},
                    'body': json.dumps({'error': 'Status check error', 'message': str(e)})
                }
        
        except Exception as e:
            print(f"ERROR - Status routing error: {e}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Status check error', 'message': str(e)})
            }
    
    # Route HTTP request to appropriate handler
    if action not in AVAILABLE_HANDLERS:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': 'Invalid action',
                'requested': action,
                'availableActions': AVAILABLE_HANDLERS
            })
        }
    
    try:
        print(f"DEBUG - Importing HTTP handler: {action}")
        handler_module = importlib.import_module(action)
        handler_func = getattr(handler_module, 'lambda_handler')
        
        print(f"DEBUG - Calling {action}.lambda_handler()")
        
        handler_event = {
            **body_data,
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
                'message': f'Could not import {action} handler'
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
                'message': f'{action}.py must have lambda_handler function'
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
                'message': str(e)
            })
        }