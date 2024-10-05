import os, fnmatch

storage_path = "../raw/"

def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(name)
    return result

files = find('*.csv', storage_path)

print(files)