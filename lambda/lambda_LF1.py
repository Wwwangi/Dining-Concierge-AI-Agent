import json
import os
import time
import dateutil
import datetime
import boto3

# Validate the user input
# ----------------------------------------------------------------------------------------------------------------------
def incorrect_field_message(field_name, error_message):
    return {
        'incorrect_field': field_name,
        'message': {'contentType': 'PlainText', 'content': error_message}
    }

def location_check(city):
    cities = ['new york', 'manhattan']
    if city.lower() not in cities:
        return incorrect_field_message('city', 'Sorry, currently we only provide dining recommendation in Manhattan area. Please try \'manhattan\'.')

def cuisine_check(dish):
    dishes = ['chinese','indian','thai','american','mexican','brunch','japanese','italian']
    if dish.lower() not in dishes:
        return incorrect_field_message('dish', 'Try cuisine in this list: chinese, indian, thai, american, mexican, brunch, japanese,italian. Thank you!')

def people_check(people):
    if not 1<=int(people)<=50:
        return incorrect_field_message('people', 'Please try again and enter a number between 1 and 50.')

def date_check(date):
    try:
        dateutil.parser.parse(date)
        d = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        if d < datetime.date.today():
            return incorrect_field_message('date', 'Please enter a valid date starting from today.')
    except:
        return incorrect_field_message('date', 'Please enter a valid date in the format yyyy/mm/dd.')

def time_check(time):
    pass

def phone_check(phone):
    if len(phone)!=10:
        return incorrect_field_message('phone', 'Sorry we currently only support phone number in the US (10 digits number).')

# ----------------------------------------------------------------------------------------------------------------------
# get the user input and check each field
def fields_check(location, cuisine, people, date, time, phone):

    if location and location_check(location):
        return location_check(location)

    if cuisine and cuisine_check(cuisine):
        return cuisine_check(cuisine)

    if people and people_check(people):
        return people_check(people)

    if date and date_check(date):
        return date_check(date)

    if time and time_check(time):
        return time_check(time)

    if phone and phone_check(phone):
        return phone_check(phone)

    return incorrect_field_message(None, None)

# ----------------------------------------------------------------------------------------------------------------------
# Elicit/delegate slot
def elicit(session_attributes, intent, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

# ----------------------------------------------------------------------------------------------------------------------
# Elicit slot if not valid, otherwise, send data to SQS
def recommendation_intent(intent_request):
    location = intent_request['currentIntent']['slots']["city"]
    cuisine = intent_request['currentIntent']['slots']["dish"]
    date = intent_request['currentIntent']['slots']["date"]
    time = intent_request['currentIntent']['slots']["time"]
    people = intent_request['currentIntent']['slots']["people"]
    phone = intent_request['currentIntent']['slots']["phone"]

    source = intent_request['invocationSource']
    print('source')
    print(source)

    if intent_request['sessionAttributes']:
        output_session_attributes = intent_request['sessionAttributes']
    else:
        output_session_attributes = {}

    requestData = {
                    "location": location,
                    "cuisine":cuisine,
                    "people": people,
                    "date":date,
                    "time": time,
                    "phone": phone
                }

    output_session_attributes['requestData'] = json.dumps(requestData)

    if source == 'DialogCodeHook':
        slots = intent_request['currentIntent']['slots']
        print(slots)
        check_results = fields_check(location, cuisine, people, date, time, phone)
        print(check_results)
        if check_results['incorrect_field']:
            slots[check_results['incorrect_field']] = None
            return elicit(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               check_results['incorrect_field'],
                               check_results['message'])
        return delegate(output_session_attributes, intent_request['currentIntent']['slots'])

    messageId = SQSRequest(requestData)

    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You’re all set. Expect my suggestions shortly by text! Have a nice day:)'})

#-----------------------------------------------------------------------------------------------------------------------
# Send data to SQS
def SQSRequest(requestData):
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/964889031791/user_preference.fifo'
    messageAttributes = {
        'location': {
                'DataType': 'String',
                'StringValue': requestData['location']
            },
        'cuisine': {
            'DataType': 'String',
            'StringValue': requestData['cuisine']
        },
        'people': {
            'DataType': 'Number',
            'StringValue': requestData['people']
        },
        "date": {
            'DataType': "String",
            'StringValue': requestData['date']
        },
        "time": {
            'DataType': "String",
            'StringValue': requestData['time']
        },
        'phone': {
            'DataType' : 'String',
            'StringValue' : requestData['phone']
        }
    }

    messageBody=('Dining Recommendation')
    print(messageAttributes)

    response = sqs.send_message(
        QueueUrl = queue_url,
        MessageAttributes = messageAttributes,
        MessageBody = messageBody,
        MessageDeduplicationId = 'messagededuplicationId1',
        MessageGroupId = 'messagegroupid1'
        )
    print(response)

    return response['MessageId']

# ----------------------------------------------------------------------------------------------------------------------

def lambda_handler(event, context):
    os.environ['TZ'] = 'US/Eastern'
    time.tzset()
    print(event)

    intent_type = event['currentIntent']['name']

    if intent_type == 'Recommendation':
        return recommendation_intent(event)