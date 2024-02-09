import re 


def format_sql_command(command):
    formatted_command = command.replace('\n', ' ')  # Remove newline characters
    formatted_command = formatted_command.strip()  # Remove leading and trailing spaces

    formatted_command = re.sub(r'\s+', ' ', formatted_command) # collapse all whitespace into single spaces at most
    
    return formatted_command