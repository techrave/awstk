#!/usr/bin/env python3
"""s3scan: Scan S3 buckets for text
Usage: s3scan.py [options] <Bucket> <Path> <FilePattern> <TextPattern>
"""

import argparse
import concurrent.futures
import json
import re
import threading
import boto3


S3DELIM = '/'

class S3Scanner: # pylint: disable=too-few-public-methods
    """
    Search S3 Buckets with Regex

    Methods
    -------
    scan()
        Performs the S3 scan
    """
    def __init__(self, bucket):
        self._bucket = bucket
        self._s3client = boto3.client('s3')
        self._s3resource = boto3.resource('s3')
        self._result = []
        self._lock = threading.Lock()

    def scan(self, path, fileregex, contentregex):
        """
        Perform the scan of the bucket

        Parameters
        ----------
        path : str
            File path in S3 bucket to start the file search
        fileregex : str
            Regular expression describing files that should be scanned
        contentregex : str
            Regular expression describing content in files that should be
            returned

        """
        files = self._find_files(path, fileregex)
        self._scan_files(files, contentregex)
        return self._result

    @staticmethod
    def _get_s3_filename(fullname):
        """
        Get filename portion of S3 path

        Parameters
        ----------
        fullname : str
            full key of S3 object

        Returns
        -------
        str
            filename portion of fullname
        """
        if fullname.endswith(S3DELIM):
            return ""

        return fullname.split(S3DELIM)[-1]

    def _find_files(self, path, regex):
        """
        Returns S3 file objects in provided path that match the regex expression

        Parameters
        ----------
        path : str
            The S3 directory to search.
        regex : str
            Regular Expression to check against file names

        Returns
        -------
        dict
            Dictionary of S3 file names with lists of relevant content found
        """
        # Handle missing / at end of prefix
        if not path.endswith(S3DELIM):
            path += S3DELIM

        retval = []
        pattern = re.compile(regex)
        paginator = self._s3client.get_paginator('list_objects_v2')
        for result in paginator.paginate(Bucket=self._bucket, Prefix=path):
            for key in result['Contents']:
                key['Filename'] = self._get_s3_filename(key['Key'])
                match = pattern.match(key['Filename'])
                if not match:
                    continue
                retval.append(key)
        return retval

    def _scan_file(self, file, regex):
        pattern = re.compile(regex)
        fobj = self._s3resource.Object(self._bucket, file['Key'])
        body = fobj.get()['Body']
        lines = []
        iterator = body.iter_lines(chunk_size=1024)
        for i in iterator:
            line = i.decode()
            match = pattern.match(line)
            if match:
                lines.append(line)
        retval = {'File': file, 'Lines': lines}
        with self._lock:
            self._result.append(retval)

    def _scan_files(self, files, regex):
        """
        Find lines in files that match the provided regex

        Parameters
        ----------
        files : list
            List of files to scan
        regex : str
            Regular expression to compare against file content
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            for file in files:
                executor.submit(self._scan_file, file, regex)

def get_args():
    """
    Parse command line arguments

    Returns
    -------
    args

    """
    parser = argparse.ArgumentParser(prog="s3scan.py")
    parser.add_argument("bucket", type=str,
                        help="S3 Bucket to scan")
    parser.add_argument("path", type=str,
                        help="Directory path in S3 bucket")
    parser.add_argument("filepattern", type=str,
                        help="Regex to identify files to scan")
    parser.add_argument("textpattern", type=str,
                        help="Regex to match file content")
    parser.add_argument("-l", "--dolambda", action="store_true",
                        help="Invoke \"s3scan\" lambda function to perform the scan")
    args = parser.parse_args()
    return args

def report(result):
    """
    Output search results

    Parameters
    ----------
    results : dict
        Scan result from S3Scanner.scan
    """
    for file in result:
        if len(file['Lines']) < 1:
            continue
        print(f"{file['File']['Key']}")
        for line in file['Lines']:
            print(f"  {line}")

def lambda_invoke(event):
    json_event = json.dumps(event)
    bytes_event = bytes(json_event, 'utf-8')
    client = boto3.client('lambda')
    result = client.invoke(FunctionName="s3scan", Payload=bytes_event)
    if result['StatusCode'] != 200:
        raise RuntimeError()

    jsonresp = json.loads(result['Payload'].read().decode('utf-8'))
    if 'errorMessage' in jsonresp:
        raise RuntimeError(jsonresp['errorMessage'])

    return jsonresp


def main():

    args = get_args()
    event = {}
    event['Bucket'] = args.bucket
    event['Path'] = args.path
    event['FilePattern'] = args.filepattern
    event['TextPattern'] = args.textpattern

    result = {}

    if args.dolambda:
        result = lambda_invoke(event)
    else:
        result = lambda_handler(event, {})

    report(result)


def lambda_prepare_result(result):
    for record in result:
        s3_file = record['File']
        s3_file['LastModified'] = str(s3_file['LastModified'])
    return result

def lambda_handler(event, context): # pylint: disable=unused-argument
    bucket = event['Bucket']
    scanner = S3Scanner(bucket)

    path = event['Path']
    file_pattern = event['FilePattern']
    text_pattern = event['TextPattern']

    matches = scanner.scan(path, file_pattern, text_pattern)

    return lambda_prepare_result(matches)

if __name__ == "__main__":
    main()
