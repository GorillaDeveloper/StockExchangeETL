import io

logs =''
def print_message(message):
    print(message)
    logs += message+"\n\r"

def write_logs_in_log_file():
    # Open the file in write mode
    with open("logs.txt", "w") as file:
        # Write the content to the file
        file.write(logs)

