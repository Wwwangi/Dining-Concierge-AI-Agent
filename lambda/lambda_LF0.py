import boto3

lex_bot = boto3.client('lex-runtime',region_name='us-east-1')

def lambda_handler(event, context):
    curr_utterance = event.get('messages')

    #To check if the posted utterance is valid
    if curr_utterance is None or len(curr_utterance) == 0:
        return {
            'statusCode': 200,
            'messages': [{
                'type': 'unstructured',
                'unstructured': {
                    'text': 'Oops! Something went wrong... Please try again later. Sorry for the inconvenience.'
                    }
                }]
            }

    # Get the user message
    message = curr_utterance[0]['unstructured']['text']
    userId = curr_utterance[0]['unstructured']['id']

    # To pass the message to lex bot (v1) and get bot response
    response = lex_bot.post_text(botName='DiningRecommendation',botAlias='Prod',userId=userId,inputText=message)

    if response['message'] and len(response['message']) != 0:
        botMessage = response['message']

    # Show the lex bot response
    return {
    'statusCode': 200,
    'messages': [{
        'type': 'unstructured',
        'unstructured': {
            'text': botMessage
            }
        }]
    }