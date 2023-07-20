import logging


# Create a logger instance
logger = logging.getLogger("my_logger")
logger.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler("logs.txt")
file_handler.setLevel(logging.INFO)

# Create a formatter and set it for the file handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)
def print_message(message):
    logger.info(message)
    print(message)

def next_line():
    print_message('\n\r')

