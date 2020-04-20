import datetime
import json
import sys

if __name__ == '__main__':
   if len(sys.argv) != 3:
      exit(1)

   input_name = sys.argv[1]
   output_name = sys.argv[2]

   insertions_loaded = True

   with open(input_name, 'r') as input_file:
      with open(output_name, 'w') as output_file:
         for line in input_file.readlines():
            if line.startswith('[') or line.startswith(']'):
               output_file.write(line)
            elif line.startswith('{'):
               if not insertions_loaded:
                  output_file.write('"insertions" : 0, "deletions" : 0 },\n')
               output_file.write(line)
               insertions_loaded = False
            else:
               insertions_loaded = True
               insertions = 0
               deletions = 0

               stats = line.split(',')
               if len(stats) != 2 and len(stats) != 3:
                  continue

               for stat in line.split(','):
                  data = stat.split(' ')

                  if len(data) != 3:
                     continue

                  if data[2].startswith('insertion'):
                     insertions = int(data[1])
                  elif data[2].startswith('deletion'):
                     deletions = int(data[1])

               output_file.write('"insertions" : {}, "deletions" : {}'.format(insertions, deletions))
               output_file.write(' },\n')
