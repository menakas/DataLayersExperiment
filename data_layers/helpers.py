import os
import logging,coloredlogs

from pathlib import Path
from prefect import get_run_logger



################### Logger ####################################################

def get_root_logger(filepath):
    
    # Set up root logger
    root_logger = logging.getLogger(__name__)
    root_logger.setLevel(logging.DEBUG)
    
    # Formatters for logs
    file_log_format    = logging.Formatter( '%(asctime)s  |  %(levelname)s  |  %(message)s  ' )
    console_log_format = coloredlogs.ColoredFormatter( fmt = '%(message)s',
                             level_styles = dict(
                                 debug    = dict( color = 'white' ),
                                 info     = dict( color = 'green' ),
                                 warning  = dict( color = 'cyan' ),
                                 error    = dict( color = 'red',   bold = True, bright     = True ),
                                 critical = dict( color = 'black', bold = True, background = 'red' )
                             ),
                             field_styles = dict(
                                 messages = dict( color = 'white' )
                             )
                         )
    
    # Create logs directory
    stage   = os.path.basename( filepath ).split( '_' )[0]
    dirpath = f'./logs/stage_{stage}/'
    os.makedirs( dirpath, exist_ok =True )
    
    # Set up File handler for logging events to file
    current_filepath = Path( filepath ).stem
    file_handler     = logging.FileHandler( dirpath + current_filepath + '.log', mode='w')
    file_handler.setFormatter( file_log_format )
    
    # Set up Console handler for logging events to console
    console_handler = logging.StreamHandler()
    console_handler.setFormatter( console_log_format )
    
    # Add the file and console handlers 
    root_logger.addHandler( file_handler )

    return root_logger
    

def log_info_msg( root_logger, message ):
  
    root_logger.info( "" )
    root_logger.info( message )
    root_logger.info( "" )
    get_run_logger().info( "" )
    get_run_logger().info( message )
    get_run_logger().info( "" )

