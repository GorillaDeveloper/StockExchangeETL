import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

string_comma_repleaces_with =' '

def Remove_Commas(txt_file):
    try:
        output_file = txt_file
        complete_text=""
        with open(txt_file, 'r') as file:
            # with open(output_file, 'w') as output:
            in_single_quotes = False
            inside_string = False
            current_string = ""
            for char in file.read():
                if char == "'":
                    if in_single_quotes:
                            in_single_quotes = False
                            inside_string = False
                            # output.write(char)
                            complete_text+=char
                            current_string = ""
                    else:
                        in_single_quotes = True
                        # output.write(char)
                        complete_text+=char
                elif char == ",":
                    if not inside_string:
                        # output.write(char)
                        complete_text+=char
                    else:
                        # output.write(string_comma_repleaces_with)
                        complete_text+=string_comma_repleaces_with
                else:
                    if in_single_quotes:
                        inside_string = True
                        current_string += char
                    # output.write(char)
                    complete_text+=char

        print("Unnecessary commas removed successfully and saved to '" + txt_file + "'.\n\r")
        # print(complete_text)
    # Usage example
        with open(output_file, 'w') as file:
            file.write(complete_text)
    except Exception as ex:
        print (ex)




