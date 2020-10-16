# CALTECH ARCHIVES AND SPECIAL COLLECTIONS DIGITAL OBJECT WORKFLOW

import base64
import boto3
import botocore
import hashlib
import json
import os
import plac
import pprint
import random
import sh
import string

from asnake.client import ASnakeClient
from datetime import datetime
from jpylyzer import jpylyzer
from requests import HTTPError
if __debug__:
    from sidetrack import set_debug, log, logr

@plac.annotations(
    collection_id = ('the collection identifier from ArchivesSpace'),
    debug = ('print extra debugging info', 'flag', '@'),
)

def main(collection_id, debug):

    if debug:
        if __debug__: set_debug(True)

    time_start= datetime.now()

    WORKDIR, COMPELTEDIR, AIP_BUCKET = get_environment_variables()

    collection_directory = get_collection_directory(WORKDIR, collection_id)
    # print(collection_directory)
    collection_uri = get_collection_uri(collection_id)
    # print(collection_uri)
    collection_json = get_collection_json(collection_uri)
    collection_json['tree']['_resolved'] = get_collection_tree(collection_uri)
    # print(collection_json)
    # Send collection metadata to S3.
    try:
        boto3.client('s3').put_object(
            Bucket=AIP_BUCKET,
            Key=collection_id + '/' + collection_id + '.json',
            Body=json.dumps(collection_json, sort_keys=True, indent=4)
        )
        print(f"✅ metadata sent to S3 for {collection_id}\n")
    except botocore.exceptions.ClientError as e:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
        if e.response['Error']['Code'] == 'InternalError': # Generic error
            # We grab the message, request ID, and HTTP code to give to customer support
            print(f"Error Message: {e.response['Error']['Message']}")
            print(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
            print(f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
        else:
            raise e

    # `depth = 2` means do not recurse past one set of subdirectories.
        # [collection]/
        # ├── [collection]_[box]_[folder]/
        # │   ├── [directory_not_traversed]/
        # │   │   └── [file_not_included].tiff
        # │   ├── [collection]_[box]_[folder]_[leaf].tiff
        # │   └── [collection]_[box]_[folder]_[leaf].tiff
        # └── [collection]_[box]_[folder]/
        #     ├── [collection]_[box]_[folder]_[leaf].tiff
        #     └── [collection]_[box]_[folder]_[leaf].tiff
    depth = 2
    filecounter = 0
    folders = []
    for root, dirs, files in os.walk(collection_directory):
        if root[len(collection_directory):].count(os.sep) == 0:
            for d in dirs:
                folders.append(os.path.join(root, d))
        if root[len(collection_directory):].count(os.sep) < depth:
            for f in files:
                # TODO(tk) set up list of usable imagetypes earlier
                if os.path.splitext(f)[1] in ['.tif', '.tiff']:
                    filecounter += 1
    filecount = filecounter

    # Loop over folders list.
    folders.sort()
    for _ in range(len(folders)):
        # Using pop() (and/or range(len()) above) maybe helps to be sure that
        # if folder metadata fails to process properly, it and its images are
        # skipped completely and the script moves on to the next folder.
        folderpath = folders.pop()
        # TODO find out how to properly catch exceptions here
        try:
            folder_arrangement, folder_data = process_folder_metadata(folderpath)
        except RuntimeError as e:
            print('❌ unable to process folder metadata...')
            print(str(e))
            print(f'...skipping {folderpath}\n')
            continue

        # Send ArchivesSpace folder metadata to S3 as a JSON file.
        try:
            boto3.client('s3').put_object(
                Bucket=AIP_BUCKET,
                Key=get_s3_aip_folder_key(get_s3_aip_folder_prefix(folder_arrangement, folder_data), folder_data),
                Body=json.dumps(folder_data, sort_keys=True, indent=4)
            )
            print(f"✅ metadata sent to S3 for {folder_data['component_id']}\n")
        except botocore.exceptions.ClientError as e:
            # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
            if e.response['Error']['Code'] == 'InternalError': # Generic error
                # We grab the message, request ID, and HTTP code to give to customer support
                print(f"Error Message: {e.response['Error']['Message']}")
                print(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
                print(f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
            else:
                raise e

        # loop over contents of folder directory
        filepaths = []
        with os.scandir(folderpath) as contents:
            for entry in contents:
                # TODO(tk) set up list of usable imagetypes earlier
                if entry.is_file() and os.path.splitext(entry.path)[1] in ['.tif', '.tiff']:
                    # print(entry.path)
                    filepaths.append(entry.path)

        # We reverse the sort for use with pop() and so the components will be
        # ingested in the correct order for the digital object tree
        filepaths.sort(reverse=True)
        for f in range(len(filepaths)):
            filepath = filepaths.pop()
            print(f'▶️  {os.path.basename(filepath)} [images remaining: {filecounter}/{filecount}]')
            filecounter -= 1
            try:
                aip_image_data = process_aip_image(filepath, collection_json, folder_arrangement, folder_data)
            except RuntimeError as e:
                print(str(e))
                continue

            # Send AIP image to S3.
            try:
                boto3.client('s3').put_object(
                    Bucket=AIP_BUCKET,
                    Key=aip_image_data['s3key'],
                    Body=open(aip_image_data['filepath'], 'rb'),
                    ContentMD5=aip_image_data['md5'],
                    Metadata={'md5': aip_image_data['md5']}
                )
            except botocore.exceptions.ClientError as e:
                # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html
                if e.response['Error']['Code'] == 'InternalError': # Generic error
                    # We grab the message, request ID, and HTTP code to give to customer support
                    print(f"Error Message: {e.response['Error']['Message']}")
                    print(f"Request ID: {e.response['ResponseMetadata']['RequestId']}")
                    print(f"HTTP Code: {e.response['ResponseMetadata']['HTTPStatusCode']}")
                else:
                    raise e

            # Set up ArchivesSpace record.
            digital_object_component = prepare_digital_object_component(folder_data, AIP_BUCKET, aip_image_data)

            # Post Digital Object Component to ArchivesSpace.
            try:
                post_digital_object_component(digital_object_component)
            except HTTPError as e:
                print(str(e))
                print(f"❌ unable to create Digital Object Component for {folder_data['component_id']}; skipping...\n")
                print(f"⚠️  clean up {aip_image_data['s3key']} file in {AIP_BUCKET} bucket\n")
                # TODO programmatically remove file from bucket?
                continue

            # TODO log file success
            print(f'✅ {os.path.basename(filepath)} processed successfully\n')

            print(f'⏳ time elpased: {datetime.now() - time_start}\n')

###

def calculate_pixel_signature(filepath):
    return sh.cut(sh.sha512sum(sh.magick.stream('-quiet', '-map', 'rgb', '-storage-type', 'short', filepath, '-', _piped=True)), '-d', ' ', '-f', '1')

def confirm_digital_object(folder_data):
    digital_object_count = 0
    for instance in folder_data['instances']:
        if 'digital_object' in instance.keys():
            digital_object_count += 1
    if digital_object_count > 1:
        raise ValueError(f"❌ the ArchivesSpace record for {folder_data['component_id']} contains multiple digital objects")
    if digital_object_count < 1:
        # folder_data = create_digital_object(folder_data)
        raise NotImplementedError('🈳 create_digital_object() not implemented yet')
    return folder_data

def confirm_digital_object_id(folder_data):
    # returns folder_data always in case digital_object_id was updated
    for instance in folder_data['instances']:
        # TODO(tk) confirm Archives policy disallows multiple digital objects
        # TODO(tk) create script/report to periodically check for violations
        if 'digital_object' in instance.keys():
            if instance['digital_object']['_resolved']['digital_object_id'] != folder_data['component_id']:
                # TODO(tk) confirm with Archives that replacing a digital_object_id
                # is acceptable in all foreseen circumstances
                set_digital_object_id(instance['digital_object']['ref'], folder_data['component_id'])
                # TODO(tk) confirm returned folder_data includes updated id
                # if setting fails we won’t get to this step anyway
                folder_data = get_folder_data(folder_data['component_id'])
    return folder_data

def confirm_file(filepath):
    # confirm file exists and has the proper extention
    # valid extensions are: .tif, .tiff
    # NOTE: no mime type checking at this point, some TIFFs were troublesome
    if os.path.isfile(filepath):
        # print(os.path.splitext(filepath)[1])
        if os.path.splitext(filepath)[1] not in ['.tif', '.tiff']:
            print('❌  invalid file type: ' + filepath)
            exit()
    else:
        print('❌  invalid file path: ' + filepath)
        exit()

# TODO(tk)
def create_digital_object(folder_data):
    client = ASnakeClient()
    client.authorize()
    # post digital object
    # TODO(tk) be sure new digital object is indexed and returnable immediately
    folder_data = get_folder_data(folder_data['component_id'])
    return folder_data

def get_aip_image_data(filepath):
    aip_image_data = {}
    aip_image_data['filepath'] = filepath
    jpylyzer_xml = jpylyzer.checkOneFile(aip_image_data['filepath'])
    aip_image_data['filesize'] = jpylyzer_xml.findtext('./fileInfo/fileSizeInBytes')
    aip_image_data['width'] = jpylyzer_xml.findtext('./properties/jp2HeaderBox/imageHeaderBox/width')
    aip_image_data['height'] = jpylyzer_xml.findtext('./properties/jp2HeaderBox/imageHeaderBox/height')
    aip_image_data['standard'] = jpylyzer_xml.findtext('./properties/contiguousCodestreamBox/siz/rsiz')
    aip_image_data['transformation'] = jpylyzer_xml.findtext('./properties/contiguousCodestreamBox/cod/transformation')
    aip_image_data['quantization'] = jpylyzer_xml.findtext('./properties/contiguousCodestreamBox/qcd/qStyle')
    # aip_image_data['md5'] = str(sh.base64(sh.openssl.md5('-binary', aip_image_data['filepath']))).strip()
    aip_image_data['md5'] = base64.b64encode(hashlib.md5(open(aip_image_data['filepath'], 'rb').read()).digest()).decode()
    return aip_image_data

def get_archival_object(id):
    client = ASnakeClient()
    client.authorize()
    response = client.get('/repositories/2/archival_objects/' + id)
    response.raise_for_status()
    return response.json()

def get_collection_directory(WORKDIR, collection_id):
    if os.path.isdir(os.path.join(WORKDIR, collection_id)):
        return os.path.join(WORKDIR, collection_id)
    else:
        print(f'❌  invalid or missing directory: {os.path.join(WORKDIR, collection_id)}')
        exit()

def get_collection_json(collection_uri):
    client = ASnakeClient()
    client.authorize()
    return client.get(collection_uri).json()

def get_collection_tree(collection_uri):
    client = ASnakeClient()
    client.authorize()
    return client.get(collection_uri + '/ordered_records').json()

def get_collection_uri(collection_id):
    client = ASnakeClient()
    client.authorize()
    search_results_json = client.get('/repositories/2/search?page=1&page_size=1&type[]=resource&fields[]=uri&aq={\"query\":{\"field\":\"identifier\",\"value\":\"' + collection_id + '\",\"jsonmodel_type\":\"field_query\",\"negated\":false,\"literal\":false}}').json()
    if bool(search_results_json['results']):
        return search_results_json['results'][0]['uri']
    else:
        print('❌  Collection Identifier not found in ArchivesSpace: ' + collection_id)
        exit()

def get_crockford_characters(n=4):
    return ''.join(random.choices('abcdefghjkmnpqrstvwxyz' + string.digits, k=n))

def get_digital_object_component_id():
    return get_crockford_characters() + '_' + get_crockford_characters()

def get_environment_variables():
    WORKDIR = os.path.abspath(os.environ.get('WORKDIR'))
    COMPLETEDIR = os.path.abspath(os.getenv('COMPLETEDIR', f'{WORKDIR}/S3'))
    AIP_BUCKET = os.environ.get('AIP_BUCKET')
    if all([WORKDIR, COMPLETEDIR, AIP_BUCKET]):
        if __debug__: log(f'WORKDIR: {WORKDIR}')
        if __debug__: log(f'COMPLETEDIR: {COMPLETEDIR}')
        if __debug__: log(f'AIP_BUCKET: {AIP_BUCKET}')
    else:
        print('❌  all environment variables must be set:')
        print('➡️   WORKDIR: /path/to/directory above collection files')
        print('➡️   COMPLETEDIR: /path/to/directory for processed source files')
        print('➡️   AIP_BUCKET: name of Amazon S3 bucket for preservation files')
        print('🖥   to set variable: export VAR=value')
        print('🖥   to see value: echo $VAR')
        exit()
    return WORKDIR, COMPLETEDIR, AIP_BUCKET

def get_file_parts(filepath):
    file_parts = {}
    file_parts['filepath'] = filepath
    file_parts['filename'] = file_parts['filepath'].split('/')[-1]
    file_parts['image_id'] = file_parts['filename'].split('.')[0]
    file_parts['extension'] = file_parts['filename'].split('.')[-1]
    file_parts['folder_id'] = file_parts['image_id'].rsplit('_', 1)[0]
    file_parts['sequence'] = file_parts['image_id'].split('_')[-1]
    file_parts['component_id'] = get_digital_object_component_id()
    return file_parts

def get_folder_arrangement(folder_data):
    # returns names and identifers of the arragement levels for a folder
    folder_arrangement = {}
    folder_arrangement['repository_name'] = folder_data['repository']['_resolved']['name']
    folder_arrangement['repository_code'] = folder_data['repository']['_resolved']['repo_code']
    folder_arrangement['folder_display'] = folder_data['display_string']
    folder_arrangement['folder_title'] = folder_data['title']
    for instance in folder_data['instances']:
        if 'sub_container' in instance.keys():
            # TODO(tk) if there is no collection, we have a problem
            if 'collection' in instance['sub_container']['top_container']['_resolved'].keys():
                folder_arrangement['collection_display'] = instance['sub_container']['top_container']['_resolved']['collection'][0]['display_string']
                folder_arrangement['collection_id'] = instance['sub_container']['top_container']['_resolved']['collection'][0]['identifier']
            if 'series' in instance['sub_container']['top_container']['_resolved'].keys():
                folder_arrangement['series_display'] = instance['sub_container']['top_container']['_resolved']['series'][0]['display_string']
                folder_arrangement['series_id'] = instance['sub_container']['top_container']['_resolved']['series'][0]['identifier']
                for ancestor in folder_data['ancestors']:
                    if ancestor['level'] == 'subseries':
                        subseries = get_archival_object(ancestor['ref'].split('/')[-1])
                        folder_arrangement['subseries_display'] = subseries['display_string']
                        folder_arrangement['subseries_id'] = subseries['component_id']
    return folder_arrangement

def get_folder_data(component_id):
    # TODO find a way to populate component_id field from metadata (see HBF)
    # searches for the component_id using keyword search; excludes pui results
    client = ASnakeClient()
    client.authorize()
    response = client.get('/repositories/2/search?page=1&page_size=10&type[]=archival_object&aq={\"query\":{\"op\":\"AND\",\"subqueries\":[{\"field\":\"keyword\",\"value\":\"' + component_id + '\",\"jsonmodel_type\":\"field_query\",\"negated\":false,\"literal\":false},{\"field\":\"types\",\"value\":\"pui\",\"jsonmodel_type\":\"field_query\",\"negated\":true}],\"jsonmodel_type\":\"boolean_query\"},\"jsonmodel_type\":\"advanced_query\"}')
    response.raise_for_status()
    if len(response.json()['results']) < 1:
        # print('❌  no records with component_id: ' + component_id)
        # exit()
        raise ValueError(f'❌ no records with component_id: {component_id}')
    if len(response.json()['results']) > 1:
        # print('❌  multiple records with component_id: ' + component_id)
        # exit()
        raise ValueError(f'❌ multiple records with component_id: {component_id}')
    return json.loads(response.json()['results'][0]['json'])

def get_folder_id(filepath):
    # isolate the filename and then get the folder id
    return filepath.split('/')[-1].rsplit('_', 1)[0]

def get_s3_aip_folder_key(prefix, folder_data):
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use older_data['component_id'] directly
    folder_id_parts = folder_data['component_id'].split('_')
    folder_id = '_'.join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    return prefix + folder_id + '.json'

def get_s3_aip_folder_prefix(folder_arrangement, folder_data):
    prefix = folder_arrangement['collection_id'] + '/'
    if 'series_id' in folder_arrangement.keys():
        prefix += (folder_arrangement['collection_id']
                + '-s'
                + folder_arrangement['series_id'].zfill(2)
                + '-'
        )
        if 'series_display' in folder_arrangement.keys():
            series_display = ''.join([c if c.isalnum() else '-' for c in folder_arrangement['series_display']])
            prefix += series_display + '/'
            if 'subseries_id' in folder_arrangement.keys():
                prefix += (folder_arrangement['collection_id']
                        + '-s'
                        + folder_arrangement['series_id'].zfill(2)
                        + '-ss'
                        + folder_arrangement['subseries_id'].zfill(2)
                        + '-'
                )
                if 'subseries_display' in folder_arrangement.keys():
                    subseries_display = ''.join([c if c.isalnum() else '-' for c in folder_arrangement['subseries_display']])
                    prefix += subseries_display + '/'
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use older_data['component_id'] directly
    folder_id_parts = folder_data['component_id'].split('_')
    folder_id = '_'.join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    folder_display = ''.join([c if c.isalnum() else '-' for c in folder_arrangement['folder_display']])
    prefix += (folder_id + '-' + folder_display + '/')
    return prefix

def get_s3_aip_image_key(prefix, file_parts):
    # NOTE: '.jp2' is hardcoded as the extension
    # HaleGE/HaleGE_s02_Correspondence_and_Documents_Relating_to_Organizations/HaleGE_s02_ss0B_National_Academy_of_Sciences/HaleGE_056_07_Section_on_Astronomy/HaleGE_056_07_0001/8c38-d9cy.jp2
    # {
    #     "component_id": "me5v-z1yp",
    #     "extension": "tiff",
    #     "filename": "HaleGE_02_0B_056_07_0001.tiff",
    #     "filepath": "/path/to/archives/data/WORKDIR/HaleGE/HaleGE_02_0B_056_07_0001.tiff",
    #     "folder_id": "HaleGE_02_0B_056_07",
    #     "image_id": "HaleGE_02_0B_056_07_0001",
    #     "sequence": "0001"
    # }
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use file_parts['folder_id'] directly
    folder_id_parts = file_parts['folder_id'].split('_')
    folder_id = '_'.join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    return prefix + folder_id + '_' + file_parts['sequence'] + '/' + file_parts['component_id'] + '-lossless.jp2'

def get_xmp_dc_metadata(folder_arrangement, file_parts, folder_data, collection_json):
    xmp_dc = {}
    xmp_dc['title'] = folder_arrangement['folder_display'] + ' [image ' + file_parts['sequence'] + ']'
    # TODO(tk) check extent type for pages/images/computer files/etc
    if len(folder_data['extents']) == 1:
        xmp_dc['title'] = xmp_dc['title'].rstrip(']') + '/' + folder_data['extents'][0]['number'].zfill(4) + ']'
    xmp_dc['identifier'] = file_parts['component_id']
    xmp_dc['publisher'] = folder_arrangement['repository_name']
    xmp_dc['source'] = folder_arrangement['repository_code'] + ': ' + folder_arrangement['collection_display']
    for instance in folder_data['instances']:
        if 'sub_container' in instance.keys():
            if 'series' in instance['sub_container']['top_container']['_resolved'].keys():
                xmp_dc['source'] += ' / ' + instance['sub_container']['top_container']['_resolved']['series'][0]['display_string']
                for ancestor in folder_data['ancestors']:
                    if ancestor['level'] == 'subseries':
                        xmp_dc['source'] += ' / ' + folder_arrangement['subseries_display']
    xmp_dc['rights'] = 'Caltech Archives has not determined the copyright in this image.'
    for note in collection_json['notes']:
        if note['type'] == 'userestrict':
            if bool(note['subnotes'][0]['content']) and note['subnotes'][0]['publish']:
                xmp_dc['rights'] = note['subnotes'][0]['content']
    return xmp_dc

def post_digital_object_component(json_data):
    client = ASnakeClient()
    client.authorize()
    post_response = client.post('/repositories/2/digital_object_components', json=json_data)
    post_response.raise_for_status()
    return post_response

def prepare_digital_object_component(folder_data, AIP_BUCKET, aip_image_data):
    # MINIMAL REQUIREMENTS: digital_object and one of label, title, or date
    # FILE VERSIONS MINIMAL REQUIREMENTS: file_uri
    # 'publish': false is the default value
    digital_object_component = {
        'file_versions': [
            {
                'checksum_method': 'md5',
                'file_format_name': 'JPEG 2000',
                'use_statement': 'image-master'
            }
        ]
    }
    for instance in folder_data['instances']:
        # not checking if there is more than one digital object
        if 'digital_object' in instance.keys():
            digital_object_component['digital_object'] = {}
            digital_object_component['digital_object']['ref'] = instance['digital_object']['_resolved']['uri']
    if digital_object_component['digital_object']['ref']:
        pass
    else:
        # TODO(tk) figure out what to do if the folder has no digital objects
        print('😶 no digital object')
    digital_object_component['component_id'] = aip_image_data['component_id']
    if aip_image_data['transformation'] == '5-3 reversible' and aip_image_data['quantization'] == 'no quantization':
        digital_object_component['file_versions'][0]['caption'] = ('width: '
                                                                   + aip_image_data['width']
                                                                   + '; height: '
                                                                   + aip_image_data['height']
                                                                   + '; compression: lossless'
                                                                  )
        digital_object_component['file_versions'][0]['file_format_version'] = (aip_image_data['standard']
                                                                               + '; lossless (wavelet transformation: 5/3 reversible with no quantization)'
                                                                              )
    elif aip_image_data['transformation'] == '9-7 irreversible' and aip_image_data['quantization'] == 'scalar expounded':
        digital_object_component['file_versions'][0]['caption'] = ('width: '
                                                                   + aip_image_data['width']
                                                                   + '; height: '
                                                                   + aip_image_data['height']
                                                                   + '; compression: lossy'
                                                                  )
        digital_object_component['file_versions'][0]['file_format_version'] = (aip_image_data['standard']
                                                                               + '; lossy (wavelet transformation: 9/7 irreversible with scalar expounded quantization)'
                                                                              )
    else:
        digital_object_component['file_versions'][0]['caption'] = ('width: '
                                                                   + aip_image_data['width']
                                                                   + '; height: '
                                                                   + aip_image_data['height']
                                                                  )
        digital_object_component['file_versions'][0]['file_format_version'] = aip_image_data['standard']
    digital_object_component['file_versions'][0]['checksum'] = aip_image_data['md5']
    digital_object_component['file_versions'][0]['file_size_bytes'] = int(aip_image_data['filesize'])
    digital_object_component['file_versions'][0]['file_uri'] = 'https://' + AIP_BUCKET + '.s3-us-west-2.amazonaws.com/' + aip_image_data['s3key']
    digital_object_component['label'] = 'Image ' + aip_image_data['sequence']
    return digital_object_component

def process_aip_image(filepath, collection_json, folder_arrangement, folder_data):
    # cut out only the checksum string for the pixel stream
    sip_image_signature = sh.cut(sh.sha512sum(sh.magick.stream('-quiet', '-map', 'rgb', '-storage-type', 'short', filepath, '-', _piped=True, _bg=True), _bg=True), '-d', ' ', '-f', '1', _bg=True)
    aip_image_path = os.path.splitext(filepath)[0] + '-LOSSLESS.jp2'
    aip_image_conversion = sh.magick.convert('-quiet', filepath, '-quality', '0', aip_image_path, _bg=True)
    file_parts = get_file_parts(filepath)
    # if __debug__: log('file_parts ⬇️'); print(json.dumps(file_parts, sort_keys=True, indent=4))
    xmp_dc = get_xmp_dc_metadata(folder_arrangement, file_parts, folder_data, collection_json)
    # print(json.dumps(xmp_dc, sort_keys=True, indent=4))
    aip_image_conversion.wait()
    write_xmp_metadata(aip_image_path, xmp_dc)
    # cut out only the checksum string for the pixel stream
    aip_image_signature = sh.cut(sh.sha512sum(sh.magick.stream('-quiet', '-map', 'rgb', '-storage-type', 'short', aip_image_path, '-', _piped=True, _bg=True), _bg=True), '-d', ' ', '-f', '1', _bg=True)
    # TODO change `get_aip_image_data()` to `get_initial_aip_image_data()`
    aip_image_data = get_aip_image_data(aip_image_path)
    sip_image_signature.wait()
    aip_image_signature.wait()
    # verify image signatures match
    if aip_image_signature != sip_image_signature:
        raise RuntimeError(f"❌  image signatures did not match: {file_parts['image_id']}")
    aip_image_s3key = get_s3_aip_image_key(get_s3_aip_folder_prefix(folder_arrangement, folder_data), file_parts)
    # if __debug__: log(f'🔑 aip_image_s3key: {aip_image_s3key}')
    # Add more values to `aip_image_data` dictionary.
    aip_image_data['component_id'] = file_parts['component_id']
    aip_image_data['sequence'] = file_parts['sequence']
    aip_image_data['s3key'] = aip_image_s3key
    if __debug__: log('aip_image_data ⬇️'); print(json.dumps(aip_image_data, sort_keys=True, indent=4))
    return aip_image_data

def process_folder_metadata(folderpath):
    print(f'📂 {os.path.basename(folderpath)}\n')

    # TODO find out how to properly catch exceptions here
    try:
        # TODO(tk) consider renaming folder_data to folder_result
        folder_data = get_folder_data(os.path.basename(folderpath)) # NOTE: different for Hale
    except ValueError as e:
        raise RuntimeError(str(e))

    try:
        folder_data = confirm_digital_object(folder_data)
    except ValueError as e:
        raise RuntimeError(str(e))
    except NotImplementedError as e:
        raise RuntimeError(str(e))

    try:
        folder_data = confirm_digital_object_id(folder_data)
    except HTTPError as e:
        raise RuntimeError(str(e))

    try:
        folder_arrangement = get_folder_arrangement(folder_data)
    except HTTPError as e:
        raise RuntimeError(str(e))

    return folder_arrangement, folder_data

def put_s3_object(bucket, key, data):
    # abstract enough for preservation and access files
    response = boto3.client('s3').put_object(
        Bucket=bucket,
        Key=key,
        Body=open(data['filepath'], 'rb'),
        ContentMD5=data['md5'],
        Metadata={'md5': data['md5']}
    )
    return response

def set_digital_object_id(uri, id):
    # raises an HTTPError exception if unsuccessful
    client = ASnakeClient()
    client.authorize()
    get_response_json = client.get(uri).json()
    get_response_json['digital_object_id'] = id
    post_response = client.post(uri, json=get_response_json)
    post_response.raise_for_status()
    return

def write_xmp_metadata(filepath, metadata):
    # NOTE: except `source` all the dc elements here are keywords in exiftool
    return sh.exiftool(
        '-title=' + metadata['title'],
        '-identifier=' + metadata['identifier'],
        '-XMP-dc:source=' + metadata['source'],
        '-publisher=' + metadata['publisher'],
        '-rights=' + metadata['rights'],
        '-overwrite_original',
        filepath
    )

###

if __name__ == "__main__":
    plac.call(main)
