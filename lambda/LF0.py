import boto3

lex_bot = boto3.client('lex-runtime')

########## Main Handler  ##########

def lambda_handler(event, context):
    curr_utterance = event['messages']

    if not curr_utterance or len(curr_utterance) == 0:
        return {
            'statusCode': 200,
            'messages': [{
                'type': 'unstructured',
                'unstructured': {
                    'text': 'Oops! Something went wrong... No text information.'
                    }
                }]
            }

    message = curr_utterance[0]['unstructured']['text']

    response = lex_bot.post_text(botName='DiningBot', botAlias="testone", userId="user001" ,inputText=message)
    
    if response['message'] and len(response['message']) != 0:
        botMessage = response['message']

    return {
    'statusCode': 200,
    'messages': [{
        'type': 'unstructured',
        'unstructured': {
            'text': botMessage
            }
        }]
    }