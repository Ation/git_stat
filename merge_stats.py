import datetime
import json
import sys

if __name__ == '__main__':
   if len(sys.argv) < 4:
      exit(1)

   output_base_name = sys.argv[1]

   commits = []
   merges = []

   commits_hashes = set()
   merge_hashes = set()

   for i in range(2, len(sys.argv)):
      base_name = sys.argv[i]

      commits_file_name = base_name + '.json'
      merges_file_name = base_name + '_merges.json'

      print('loading {}'.format(commits_file_name))
      with open(commits_file_name, 'r') as input_file:
         loaded_commits = json.load(input_file)
      print('loading {}'.format(merges_file_name))
      with open(merges_file_name, 'r') as input_file:
         loaded_merges = json.load(input_file)

      print('Merging commits')
      for c in loaded_commits:
         if 'hash' in c:
            if c['hash'] not in commits_hashes:
               commits_hashes.add(c['hash'])
               commits.append(c)

      print('Merging merged')
      for c in loaded_merges:
         if 'hash' in c:
            if c['hash'] not in merge_hashes:
               merge_hashes.add(c['hash'])
               merges.append(c)

   out_commits_file_name = output_base_name + '.json'
   out_merges_file_name = output_base_name + '_merges.json'

   print('Dumping commits to {}'.format(out_commits_file_name))
   with open(out_commits_file_name, 'w') as output_file:
      json.dump(commits, output_file)

   print('Dumping merged to {}'.format(out_merges_file_name))
   with open(out_merges_file_name, 'w') as output_file:
      json.dump(merges, output_file)
