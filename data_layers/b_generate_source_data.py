import os
import json
import time
import uuid
import random
import configparser
import pandas as pd
from faker import Faker
from datetime import date
from dateutil.relativedelta import relativedelta

from helpers import get_root_logger, log_info_msg

# Get root logger
root_logger = get_root_logger( __file__ )


def generate_hotel_data():
    
    # Constants for sample data generation

    NUM_OF_GUESTS     = 5000
    NUM_OF_ROOMS      = 200
    NUM_OF_BOOKINGS   = 10000
    NUM_OF_BILLS      = 15000
    NUM_OF_REVIEWS    = 1000

    START_DATE        = date.today().replace( day = 1 ) + relativedelta( months = -3 )
    END_DATE          = date.today().replace( day = 1 ) + relativedelta( months = +7 ) + relativedelta( days = -1 )
    log_info_msg( root_logger, START_DATE )
    log_info_msg( root_logger, END_DATE )

    # Set up environment variables 
    config = configparser.ConfigParser()
    path = os.path.abspath( './config.ini' )
    config.read( path )

    DATA_PATH = config[ 'hotel_data_filepath' ][ 'DATA_PATH' ]


    ################## Guests ##################################################

    GUEST_RECORDS_PROCESSING_START_TIME = time.time()

    guest_df = pd.DataFrame( generate_guest_records( NUM_OF_GUESTS ) )
    log_info_msg( root_logger, guest_df )

    # Write dataframe to JSON file
    os.makedirs( f'{DATA_PATH}', exist_ok = True )
    with open( f'{DATA_PATH}guests.json', 'w' ) as guest_file:
        guest_df_to_json = json.loads( guest_df.to_json( orient = "records", default_handler = str ) )
        guest_file.write( json.dumps( guest_df_to_json, indent = 4, sort_keys = True ) )

    log_info_msg( root_logger, '============================ CUSTOMER INFORMATION ============================' )
    log_info_msg( root_logger, guest_df )

    GUEST_RECORDS_PROCESSING_END_TIME = time.time()


    ################## Rooms ##################################################

    ROOM_RECORDS_PROCESSING_START_TIME = time.time()

    room_df = pd.DataFrame( generate_room_records( NUM_OF_ROOMS ) )
    log_info_msg( root_logger, room_df )

    # Write dataframe to JSON file
    os.makedirs( f'{DATA_PATH}', exist_ok = True )
    with open( f'{DATA_PATH}rooms.json', 'w' ) as room_file:
        room_df_to_json = json.loads( room_df.to_json( orient = "records", default_handler = str ) )
        room_file.write( json.dumps( room_df_to_json, indent = 4, sort_keys = True ) )

    log_info_msg( root_logger, '============================ ROOM INFORMATION ============================' )
    log_info_msg( root_logger, room_df )

    ROOM_RECORDS_PROCESSING_END_TIME = time.time()


    ################## Bookings  ##################################################

    BOOKING_TYPE_RECORDS_PROCESSING_START_TIME = time.time()

    booking_df = pd.DataFrame( generate_booking_records( NUM_OF_BOOKINGS ) )
    log_info_msg( root_logger, booking_df )

    # Write dataframe to JSON file
    os.makedirs( f'{DATA_PATH}', exist_ok = True )
    with open( f'{DATA_PATH}bookings.json', 'w' ) as booking_file:
        booking_df_to_json = json.loads( booking_df.to_json( orient = "records", default_handler = str ) )
        booking_file.write( json.dumps( booking_df_to_json, indent = 4, sort_keys = True ) )

    log_info_msg( root_logger, '============================ BOOKING INFORMATION ============================' )
    log_info_msg( root_logger, booking_df )

    BOOKING_RECORDS_PROCESSING_END_TIME = time.time()


    ################## Bills  ##################################################

    BILL_TYPE_RECORDS_PROCESSING_START_TIME = time.time()

    bill_df = pd.DataFrame( generate_bill_records( NUM_OF_BILLS ) )
    log_info_msg( root_logger, bill_df )

    # Write dataframe to JSON file
    os.makedirs( f'{DATA_PATH}', exist_ok = True )
    with open( f'{DATA_PATH}bills.json', 'w' ) as bill_file:
        bill_df_to_json = json.loads( bill_df.to_json( orient = "records", default_handler = str ) )
        bill_file.write( json.dumps( bill_df_to_json, indent = 4, sort_keys = True ) )

    log_info_msg( root_logger, '============================ BILL INFORMATION ============================' )
    log_info_msg( root_logger, bill_df )

    BILL_RECORDS_PROCESSING_END_TIME = time.time()


    ################## Reviews  ##################################################

    REVIEW_RECORDS_PROCESSING_START_TIME = time.time()

    review_df = pd.DataFrame( generate_review_records( NUM_OF_REVIEWS ) )
    log_info_msg( root_logger, review_df )

    # Write dataframe to JSON file
    os.makedirs( f'{DATA_PATH}', exist_ok = True )
    with open( f'{DATA_PATH}reviews.json', 'w' ) as review_file:
        review_df_to_json = json.loads( review_df.to_json( orient = "records", default_handler = str ) )
        review_file.write( json.dumps( review_df_to_json, indent = 4, sort_keys = True ) )

    log_info_msg( root_logger, '============================ REVIEW INFORMATION ============================' )
    log_info_msg( root_logger, review_df )

    REVIEW_RECORDS_PROCESSING_END_TIME = time.time()


def generate_guest_records( guests ):
    # Create a Faker instance to generate fake data
    fake = Faker()

    created_date = fake.date_this_decade()

    for i in range( guests ):
        first_name = fake.first_name()
        last_name = fake.last_name()
        guest_record = { 
            'guest_id': 'guest_' + str( uuid.uuid4().hex ),
            'first_name': first_name,
            'last_name': last_name, 
            'email': f"{first_name.lower()}.{last_name.lower()}@" + random.choice( [
                'yoyo.com',
                'fifo.com',
                'giggle.com',
                'mosquito.net',
                'macrohard.com',
                'bumblebee.org',
                'pattukutti.in',
                'music-mania.org'
            ] ),
            'dob' : fake.date_of_birth(),
            'address': fake.address(),
            'city': fake.city(),
            'state': fake.state(),
            'zip': fake.zipcode(),
            'phone_number': fake.phone_number(),
            'credit_card': fake.credit_card_number(),
            'credit_card_provider': fake.credit_card_provider(),
            'nationality': fake.country(),
            'created_date': created_date,
            'last_updated_date': created_date + pd.Timedelta( days = random.randint( 1, 40 ) ),
            'preferred_contact_method': random.choice( [
                 "Email",
                 "Phone",
                 "Text message",
                 "Post",
                 "No contact"
            ] )
            }   
        yield guest_record

def generate_room_records( rooms ):
    # Create a Faker instance to generate fake data
    fake = Faker()

    created_date = fake.date_this_decade()

    length = len( str( rooms ) )
    for i in range( rooms ):
        room_record = { 
            'room_id': 'room_' + str( i + 1 ).zfill( length ),
            'type': random.choice( [ 'Standard', 'Deluxe', 'Suite' ] ),
            'description': random.choice( [ 'Standard', 'Deluxe', 'Suite' ] ),
            'occupancy': random.choice( [ 'Single', 'Double' ] ),
            'image_urls': get_image_urls( 10 )
            }   
        yield room_record

def get_image_urls( num ):
    iurls = []
    for i in range( num ):
        fake = Faker(i)
        iurl = fake.image_url()
        while "placekitten" not in iurl:
            iurl = fake.image_url()
        iurls.append(iurl)
    return iurls
   
def generate_bill_records( bills ):
    # Create a Faker instance to generate fake data
    fake = Faker()

    created_date = fake.date_this_decade()

    for i in range( bills ):
        first_name = fake.first_name()
        last_name = fake.last_name()
        bill_record = { 
            'bill_id': 'bill_' + str( uuid.uuid4().hex ),
            }   
        yield bill_record

def generate_booking_records( bookings ):
    # Create a Faker instance to generate fake data
    fake = Faker()

    created_date = fake.date_this_decade()

    for i in range( bookings ):
        first_name = fake.first_name()
        last_name = fake.last_name()
        booking_record = { 
            'booking_id': 'booking_' + str( uuid.uuid4().hex ),
            }   
        yield booking_record

def generate_review_records( reviews ):
    # Create a Faker instance to generate fake data
    fake = Faker()

    created_date = fake.date_this_decade()

    for i in range( reviews ):
        first_name = fake.first_name()
        last_name = fake.last_name()
        review_record = { 
            'review_id': 'review_' + str( uuid.uuid4().hex ),
            }   
        yield review_record

generate_hotel_data()
