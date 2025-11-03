import json
import boto3


def lambda_handler(event, context):
    """
    Provides decision support and recommendations based on law analysis.

    Expected event structure:
    {
        "analysis_data": {...law analysis output...},
        "decision_context": "investment|policy_response|business_strategy|compliance",
        "stakeholder": "investor|regulator|company|consumer"
    }

    Returns:
    HTTP response with decision recommendations
    """

    try:
        # Initialize Bedrock client
        bedrock = boto3.client('bedrock-runtime', region_name='us-west-2')

        # Extract parameters from event
        analysis_data = event.get('analysis_data', {})
        if not analysis_data:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Bad Request',
                    'message': 'analysis_data is required'
                })
            }

        decision_context = event.get('decision_context', 'business_strategy')
        stakeholder = event.get('stakeholder', 'company')

        # System prompt for decision support
        system_prompt = [{
            "text": f"""You are an expert strategic advisor specializing in decision support for the {stakeholder} stakeholder.

Your role is to:
- Provide clear, actionable recommendations
- Assess risk-reward tradeoffs
- Identify key decision points
- Suggest contingency plans
- Prioritize actions based on impact and urgency

Context: {decision_context}
Stakeholder: {stakeholder}

Guidelines:
- Be decisive and clear
- Base recommendations on data
- Consider both immediate and long-term effects
- Identify dependencies and critical success factors"""
        }]

        # User message with analysis
        user_message = [{
            "role": "user",
            "content": [{
                "text": f"""Based on this law analysis, provide strategic decision recommendations for a {stakeholder} in the context of {decision_context}:

Analysis Data:
{json.dumps(analysis_data, indent=2)}

Provide:
1. Top 3-5 key decisions to make
2. Risk assessment for each decision
3. Expected outcomes and timeline
4. Resource requirements
5. Success metrics and KPIs
6. Contingency plans for adverse scenarios"""
            }]
        }]

        # Call Bedrock
        response = bedrock.converse(
            modelId="global.anthropic.claude-haiku-4-5-20251001-v1:0",
            system=system_prompt,
            messages=user_message,
            inferenceConfig={
                "maxTokens": 2048,
                "temperature": 0.3
            }
        )

        # Extract response
        content = response['output']['message']['content']
        recommendations = content[0]['text'] if content else "No recommendations generated"

        # Build and return the successful result
        return {
            'statusCode': 200,
            'body': json.dumps({
                'decision_context': decision_context,
                'stakeholder': stakeholder,
                'recommendations': recommendations,
                'metadata': {
                    'model_used': 'claude-haiku-4-5-20251001-v1:0'
                },
                'usage': {
                    'input_tokens': response['usage']['inputTokens'],
                    'output_tokens': response['usage']['outputTokens']
                }
            })
        }

    except ValueError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Bad Request',
                'message': str(e)
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': f'Error generating recommendations: {str(e)}'
            })
        }