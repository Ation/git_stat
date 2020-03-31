import datetime
import json

input_name = 'git_info.data'
output_name = 'git_info.json'

with input_file as open(input_name, 'r'):
   with output_file as open(output_name, 'w'):
      for line in input_file.readlines():
         if line.startswith('[') or line.startswith(']') or line.startswith('{'):
            output_file.writeline()

