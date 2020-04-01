import datetime
import json
import sys

if __name__ == '__main__':
   if len(sys.argv) != 3:
      exit(1)

   input_name = sys.argv[1]
   output_name = sys.argv[2]

   with open(input_name, 'r') as input_file:
      with open(output_name, 'w') as output_file:
         for line in input_file.readlines():
            if line.startswith('[') or line.startswith(']') or line.startswith('{'):
               output_file.write(line)
            else:
               insertions = 0
               deletions = 0

               stats = line.split(',')
               if len(stats) != 2 and len(stats) != 3:
                  continue

               for stat in line.split(','):
                  data = stat.split(' ')

                  if len(data) != 3:
                     continue

                  if data[2].startswith('insertions'):
                     insertions = int(data[1])
                  elif data[2].startswith('deletions'):
                     deletions = int(data[1])

               output_file.write('"insertions" : {}, "deletions" : {}'.format(insertions, deletions))
               output_file.write(' },\n')
