import math
import dateutil.parser
import datetime
import time
import os
import logging
import sys
import boto3
from botocore.exceptions import ClientError

########## Built-up Functions ##########

def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


########## Validation Functions for User Input ##########

def build_validation_result(is_valid, violated_slot, message_content):
    if message_content is None:
        return {
            "isValid": is_valid,
            "violatedSlot": violated_slot,
        }

    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def isvalid_city(city):
    for char in city:
        if not char.isalpha() and char != ' ':
            return False
    return True


def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True

    except ValueError:
        return False


def validate_suggest_restaurant(slot):
    city = slot["city"]
    cuisine = slot["cuisine"]
    people = slot["people"]
    date = slot["date"]
    time = slot["time"]
    phone = slot["phone"]
    
    cities = ['new york', 'manhattan', 'ny']
    cuisines = ['caribbean', 'japanese', 'italian', 'chinese', 'american', 'mexico', 'korean']

    if city:
        # city contains only char and space
        if not isvalid_city(city):
            return build_validation_result(False, 'city', 'I did not understand that, what city would you like to check?')
        else:
            if city.lower() not in cities:
                return build_validation_result(False, 'city', 'We do not have data at {}. Our most popular cuisine are in new york, and manhattan'.format(city))

    if cuisine:
        if cuisine.lower() not in cuisines:
            return build_validation_result(False,
                                           'cuisine',
                                           'We do not have {}, would you like a different type of cuisines?'
                                           'Our most popular cuisine are caribbean, japanese, italian, chinese, american, mexico, and korean'.format(cuisine))

    if date:
        if not isvalid_date(date):
            return build_validation_result(False, 'date', 'I did not understand that, what date would you like to check?')

        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'date', 'What time?')


    if phone:
        if not phone.isdigit() or len(phone) < 9 or len(phone) > 13:
            return build_validation_result(False, 'phone', 'The phone number entered is not a valid phone number '.format(phone))


    if time:
        if len(time) != 5:
            return build_validation_result(False, 'time', None)

        hour, minute = time.split(':')
        
        if not hour.isdigit() or not minute.isdigit():
            return build_validation_result(False, 'time', None)
        
        hour, minute = int(hour), int(minute)

        if hour < 9 or hour > 21:
            return build_validation_result(False, 'time', 'Our business hours are from 9 a m. to 9 p m. Can you specify a time during this range?')

    if people:
        if not people.isdigit():
            return build_validation_result(False, 'people', 'Please enter a number.')
        
        if int(people) > 30 or int(people) < 1:
            return build_validation_result(False, 'people', 'Your number {} is invalid. Please enter a number between 1 and 30'.format(people))

    return build_validation_result(True, None, None)


########## Main Suggestion Operation ##########

def suggest_restaurant(intent_request):
    source = intent_request['invocationSource']
    slots = intent_request['currentIntent']['slots']

    if source == 'DialogCodeHook':
        validation_result = validate_suggest_restaurant(slots)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(intent_request['sessionAttributes'],
                               intent_request['currentIntent']['name'],
                               slots,
                               validation_result['violatedSlot'],
                               validation_result['message'])

        output_session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] else {}
        return delegate(output_session_attributes, slots)

    # Send Message to SQS
    sqs_response = message_sender(slots)

    print("Send slot to SQS successfully.") if sqs_response else print("Fail to send slot to SQS.")

    return close(intent_request['sessionAttributes'],
                 'Fulfilled',
                 {'contentType': 'PlainText',
                  'content': 'You\'re all set. Expect my suggestions shortly! Have a good day.'})


def message_sender(slots):
    try:
        sqs_client = boto3.client('sqs')
        response = sqs_client.send_message(
            QueueUrl = 'https://sqs.us-east-1.amazonaws.com/558392889561/DiningSQS.fifo',
            # mark for fifo queue, DelaySeconds=0
            DelaySeconds = 0,
            MessageBody = 'Test send message',
            MessageAttributes = {
                'city': {
                    'DataType': 'String',
                    'StringValue': slots['city']
                },
                'cuisine': {
                    'DataType': 'String',
                    'StringValue': slots['cuisine']
                },
                'people': {
                    'DataType': 'String',
                    'StringValue': slots['people']
                },
                'date': {
                    'DataType': 'String',
                    'StringValue': slots['date']
                },
                'time': {
                    'DataType': 'String',
                    'StringValue': slots['time']
                },
                'phone': {
                    'DataType': 'String',
                    'StringValue': slots['phone']
                }},
            MessageDeduplicationId='chatbotfall2021_cw3326',
            MessageGroupId='123456789987654321')
        
        return True
    
    except ClientError:
        return False


########## Intents ##########

def dispatch(intent_request):

    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'GreetingIntent':
        return close(intent_request['sessionAttributes'], 'Fulfilled', {'contentType': 'PlainText', 'content': 'Hi there, how can I help?'})

    elif intent_name == 'DiningSuggestionsIntent':
        return suggest_restaurant(intent_request)

    elif intent_name == 'ThankYouIntent':
        return close(intent_request['sessionAttributes'], 'Fulfilled', {'contentType': 'PlainText', 'content': 'You`re welcome.'})

    raise Exception('Intent with name ' + intent_name + ' not supported')


########## Main handler ##########

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()

    return dispatch(event)
