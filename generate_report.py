import sys

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Enter repo name')
        exit(1)

    repo_name = sys.argv[1]
