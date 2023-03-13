from helpers import get_root_logger, log_info_msg
from prefect import task, flow


# Get root logger
root_logger = get_root_logger( __file__ )


################### TASKS #####################################################

################### Prepare - Generate Sample Data ############################

@task
def generate_source_data():
    from data_layers.b_generate_source_data import generate_hotel_data
    module = 'data_layers.b_generate_source_data'
    function = 'generate_hotel_data'
    msg = eval( 'f' + repr( "Importing '{function}' function from '{module}' module..." ) ) 
    log_info_msg( root_logger, msg )


################### Layer 1 - Ingest Raw Data ############################

@task
def ingest_raw_data_rooms_table():
    from data_layers.c_layer1_ingest_raw_data import ingest_rooms_data
    module = 'data_layers.c_layer1_ingest_raw_data'
    function = 'ingest_rooms_data'
    msg = eval( 'f' + repr( "Importing '{function}' function from '{module}' module..." ) ) 
    log_info_msg( root_logger, msg )



################### FLOWS ###########################################

################### Prepare - Generate Sample Data ############################

@flow(name="Generate hotel data", flow_run_name="generate_hotel_data_flow")
def run_generate_data_flow():
    generate_source_data()
    log_info_msg( root_logger, "SUCCESS! Completed generating data for hotel bookings! " )



################### RUN PIPELINE ###############################################


# Execute linear pipeline
if __name__=="__main__":

    # Prepare - Generate Sample Data
    run_generate_data_flow()


    # Layer 1 - Ingest raw source data
    #run_ingest_data_flow()
    

    # Layer 2 - Clean raw source data
    #run_clean_data_flow()
