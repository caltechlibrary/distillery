# CALTECH ARCHIVES AND SPECIAL COLLECTIONS DIGITAL OBJECT WORKFLOW

import base64
import boto3
import hashlib
import json
import os
import random
import sh
import string

from asnake.client import ASnakeClient
from jpylyzer import jpylyzer

def check_environment_variables():
    WORKDIR = os.environ.get('WORKDIR')
    AIP_BUCKET = os.environ.get('AIP_BUCKET')
    if all([WORKDIR, AIP_BUCKET]):
        pass
    else:
        print('❌  all environment variables must be set:')
        print('  WORKDIR: /path/to/directory above collection files')
        print('  AIP_BUCKET: name of Amazon S3 bucket for preservation files')
        print('🖥   to set variable: export VAR=value')
        print('🖥   to see value: echo $VAR')
        exit()

def get_collection_directory(collection_id):
    WORKDIR = os.getenv('WORKDIR').rstrip(os.path.sep)
    if os.path.isdir(WORKDIR + os.path.sep + collection_id):
        return WORKDIR + os.path.sep + collection_id
    else:
        print('❌  invalid or missing directory: ' + WORKDIR + os.path.sep + collection_id)
        exit()

def get_collection_uri(collection_id):
    client = ASnakeClient()
    client.authorize()
    search_results_json = client.get('/repositories/2/search?page=1&page_size=1&type[]=resource&fields[]=uri&aq={\"query\":{\"field\":\"identifier\",\"value\":\"' + collection_id + '\",\"jsonmodel_type\":\"field_query\",\"negated\":false,\"literal\":false}}').json()
    if bool(search_results_json['results']):
        return search_results_json['results'][0]['uri']
    else:
        print('❌  Collection Identifier not found in ArchivesSpace: ' + collection_id)
        exit()

def get_collection_json(collection_uri):
    client = ASnakeClient()
    client.authorize()
    return client.get(collection_uri).json()

def get_collection_tree(collection_id):
    client = ASnakeClient()
    client.authorize()
    return client.get(collection_uri + '/ordered_records').json()

# def save_collection_data(directory, json):
#     with open(directory.split(os.path.sep)[-1] + '_collection_data.json', 'w') as f:
#         json.dump(json, f)

# def save_collection_tree(directory, json):
#     with open(directory.split(os.path.sep)[-1] + '_collection_tree.json', 'w') as f:
#         json.dump(json, f)

### LOOP FUNCTIONS
# process a single image
# requires: a file path, an archival object in ArchivesSpace
# results: preservation image, access image, digital object component

### confirm file exists and has the proper extention
# valid extensions are: .tif, .tiff
# NOTE: no mime type checking at this point, some TIFFs were troublesome
def confirm_file(filepath):
    if os.path.isfile(filepath):
        # print(os.path.splitext(filepath)[1])
        if os.path.splitext(filepath)[1] not in ['.tif', '.tiff']:
            print('❌  invalid file type: ' + filepath)
            exit()
    else:
        print('❌  invalid file path: ' + filepath)
        exit()

def get_folder_id(filepath):
    # isolate the filename and then get the folder id
    return filepath.split('/')[-1].rsplit('_', 1)[0]

def get_folder_data(component_id):
    # searches for the component_id using keyword search; excludes pui results
    client = ASnakeClient()
    client.authorize()
    response = client.get('/repositories/2/search?page=1&page_size=10&type[]=archival_object&aq={\"query\":{\"op\":\"AND\",\"subqueries\":[{\"field\":\"keyword\",\"value\":\"' + component_id + '\",\"jsonmodel_type\":\"field_query\",\"negated\":false,\"literal\":false},{\"field\":\"types\",\"value\":\"pui\",\"jsonmodel_type\":\"field_query\",\"negated\":true}],\"jsonmodel_type\":\"boolean_query\"},\"jsonmodel_type\":\"advanced_query\"}').json()
    if len(response['results']) < 1:
        print('❌  no records with component_id: ' + component_id)
        exit()
    if len(response['results']) > 1:
        print('❌  multiple records with component_id: ' + component_id)
        exit()
    return json.loads(response['results'][0]['json'])

def get_arrangement_parts(folder_data):
    # returns names and identifers of the arragement levels for a folder
    arrangement_parts = {}
    arrangement_parts['repository_name'] = folder_data['repository']['_resolved']['name']
    arrangement_parts['repository_code'] = folder_data['repository']['_resolved']['repo_code']
    arrangement_parts['folder_display'] = folder_data['display_string']
    arrangement_parts['folder_title'] = folder_data['title']
    for instance in folder_data['instances']:
        if 'sub_container' in instance.keys():
            # TODO(tk) if there is no collection, we have a problem
            if 'collection' in instance['sub_container']['top_container']['_resolved'].keys():
                arrangement_parts['collection_display'] = instance['sub_container']['top_container']['_resolved']['collection'][0]['display_string']
                arrangement_parts['collection_id'] = instance['sub_container']['top_container']['_resolved']['collection'][0]['identifier']
            if 'series' in instance['sub_container']['top_container']['_resolved'].keys():
                arrangement_parts['series_display'] = instance['sub_container']['top_container']['_resolved']['series'][0]['display_string']
                arrangement_parts['series_id'] = instance['sub_container']['top_container']['_resolved']['series'][0]['identifier']
                for ancestor in folder_data['ancestors']:
                    if ancestor['level'] == 'subseries':
                        subseries = get_archival_object(ancestor['ref'].split('/')[-1])
                        arrangement_parts['subseries_display'] = subseries['display_string']
                        arrangement_parts['subseries_id'] = subseries['component_id']
    return arrangement_parts

def get_crockford_characters(n=4):
    return ''.join(random.choices('abcdefghjkmnpqrstvwxyz' + string.digits, k=n))

def get_digital_object_component_id():
    return get_crockford_characters() + '_' + get_crockford_characters()

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

def calculate_pixel_signature(filepath):
    return sh.cut(sh.sha512sum(sh.magick.stream('-quiet', '-map', 'rgb', '-storage-type', 'short', filepath, '-', _piped=True)), '-d', ' ', '-f', '1')

def get_archival_object(id):
    client = ASnakeClient()
    client.authorize()
    response = client.get('/repositories/2/archival_objects/' + id).json()
    return response

def get_xmp_dc_metadata(arrangement_parts, file_parts, folder_data, collection_json):
    xmp_dc = {}
    xmp_dc['title'] = arrangement_parts['folder_display'] + ' [image ' + file_parts['sequence'] + ']'
    # TODO(tk) check extent type for pages/images/computer files/etc
    if len(folder_data['extents']) == 1:
        xmp_dc['title'] = xmp_dc['title'].rstrip(']') + '/' + folder_data['extents'][0]['number'].zfill(4) + ']'
    xmp_dc['identifier'] = file_parts['component_id']
    xmp_dc['publisher'] = arrangement_parts['repository_name']
    xmp_dc['source'] = arrangement_parts['repository_code'] + ': ' + arrangement_parts['collection_display']
    for instance in folder_data['instances']:
        if 'sub_container' in instance.keys():
            if 'series' in instance['sub_container']['top_container']['_resolved'].keys():
                xmp_dc['source'] += ' / ' + instance['sub_container']['top_container']['_resolved']['series'][0]['display_string']
                for ancestor in folder_data['ancestors']:
                    if ancestor['level'] == 'subseries':
                        xmp_dc['source'] += ' / ' + arrangement_parts['subseries_display']
    xmp_dc['rights'] = 'Caltech Archives has not determined the copyright in this image.'
    for note in collection_json['notes']:
        if note['type'] == 'userestrict':
            if bool(note['subnotes'][0]['content']) and note['subnotes'][0]['publish']:
                xmp_dc['rights'] = note['subnotes'][0]['content']
    return xmp_dc

def write_xmp_metadata(imagepath, metadata):
    # NOTE: except `source` all the dc elements here are keywords in exiftool
    return sh.exiftool(
        '-title=' + metadata['title'],
        '-identifier=' + metadata['identifier'],
        '-XMP-dc:source=' + metadata['source'],
        '-publisher=' + metadata['publisher'],
        '-rights=' + metadata['rights'],
        '-overwrite_original',
        imagepath
    )

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

def get_s3_aip_image_key(arrangement_parts, file_parts):
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
    # {
    #     "collection_display": "George Ellery Hale Papers",
    #     "collection_id": "HaleGE",
    #     "folder_display": "Section on Astronomy, 1916\u20131937",
    #     "folder_title": "Section on Astronomy",
    #     "repository_code": "Caltech Archives",
    #     "repository_name": "California Institute of Technology Archives and Special Collections",
    #     "series_display": "Correspondence and Records Relating to Organizations, 1863\u20131937",
    #     "series_id": "2",
    #     "subseries_display": "National Academy of Sciences, 1902\u20131937",
    #     "subseries_id": "B"
    # }
    key = arrangement_parts['collection_id'] + '/'
    if 'series_id' in arrangement_parts.keys():
        key += (arrangement_parts['collection_id']
                + '-s'
                + arrangement_parts['series_id'].zfill(2)
                + '-'
        )
        if 'series_display' in arrangement_parts.keys():
            series_display = ''.join([c if c.isalnum() else '-' for c in arrangement_parts['series_display']])
            key += series_display + '/'
            if 'subseries_id' in arrangement_parts.keys():
                key += (arrangement_parts['collection_id']
                        + '-s'
                        + arrangement_parts['series_id'].zfill(2)
                        + '-ss'
                        + arrangement_parts['subseries_id'].zfill(2)
                        + '-'
                )
                if 'subseries_display' in arrangement_parts.keys():
                    subseries_display = ''.join([c if c.isalnum() else '-' for c in arrangement_parts['subseries_display']])
                    key += subseries_display + '/'
    # exception for extended identifiers like HaleGE_02_0B_056_07
    # TODO(tk) remove once no more exception files exist
    # TODO(tk) use file_parts['folder_id'] directly
    folder_id_parts = file_parts['folder_id'].split('_')
    folder_id = '_'.join([folder_id_parts[0], folder_id_parts[-2], folder_id_parts[-1]])
    folder_display = ''.join([c if c.isalnum() else '-' for c in arrangement_parts['folder_display']])
    key += (folder_id
            + '-'
            + folder_display
            + '/'
            + folder_id
            + '_'
            + file_parts['sequence']
            + '/'
            + file_parts['component_id']
            + '.jp2'
    )
    return key

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

def create_digital_object_component(folder_data, file_parts, AIP_BUCKET, aip_image_key, aip_image_data):
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
    # TODO(tk) think through component_id/filename matching scheme for both
        # access and preservation copies; scenario: Alice downloads a file with an
        # opaque identifier and doesn't know what it is, then she looks it up in
        # ArchivesSpace and finds the record, info about the image content is
        # acquired but info about *which file version* she has a downloaded copy
        # of still eludes her; searching in ArchivesSpace for the filename part of a
        # File URI (fwgf-nv7c.jp2, for example) will return the digital object
        # component, but searching without the extension (fwgf-nv7c) will not
    digital_object_component['component_id'] = file_parts['component_id']
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
    digital_object_component['file_versions'][0]['file_uri'] = 'https://' + AIP_BUCKET + '.s3-us-west-2.amazonaws.com/' + aip_image_key
    digital_object_component['label'] = 'Image ' + file_parts['sequence']
    return digital_object_component

def post_digital_object_component(json):
    client = ASnakeClient()
    client.authorize()
    response = client.post('/repositories/2/digital_object_components', json=json)
    return response

###

if __name__ == "__main__":

    import glob
    import os
    import sh
    import sys

    if len(sys.argv) > 1:
        collection_id = sys.argv[1]
    else:
        print('❌  missing parameter: Collection Identifier')
        exit()

    check_environment_variables()
    AIP_BUCKET = os.getenv('AIP_BUCKET')

    collection_directory = get_collection_directory(collection_id)
    collection_uri = get_collection_uri(collection_id)
    collection_json = get_collection_json(collection_uri)
    # send collection_json to S3
    # save_collection_data(collection_directory, collection_json)
    s3_put_collection_data_response = boto3.client('s3').put_object(
        Bucket=AIP_BUCKET,
        Key=collection_id + os.path.sep + collection_id + '-collection-data.json',
        Body=json.dumps(collection_json, sort_keys=True, indent=4)
    )
    print(json.dumps(s3_put_collection_data_response, sort_keys=True, indent=4))
    # send collection_tree to S3
    collection_tree = get_collection_tree(collection_id)
    # save_collection_tree(collection_directory, collection_tree)
    s3_put_collection_tree_response = boto3.client('s3').put_object(
        Bucket=AIP_BUCKET,
        Key=collection_id + os.path.sep + collection_id + '-collection-tree.json',
        Body=json.dumps(collection_tree, sort_keys=True, indent=4)
    )
    print(json.dumps(s3_put_collection_tree_response, sort_keys=True, indent=4))

    # loop over all files
    for filepath in glob.iglob(collection_directory + '/**', recursive=True):
        print('🔎  ' + filepath)
        if os.path.isfile(filepath) and os.path.splitext(filepath)[1] in ['.tif', '.tiff']:
            file_parts = get_file_parts(filepath)
            print(json.dumps(file_parts, sort_keys=True, indent=4))
            # NOTE: unsure how to run with _bg=True from a function
            sip_image_signature = sh.cut(sh.sha512sum(sh.magick.stream('-quiet', '-map', 'rgb', '-storage-type', 'short', filepath, '-', _piped=True, _bg=True), _bg=True), '-d', ' ', '-f', '1', _bg=True)
            # split off the extension from the source filepath
            aip_image_path = os.path.splitext(filepath)[0] + '-LOSSLESS.jp2'
            # NOTE: unsure how to run with _bg=True from a function
            aip_image_conversion = sh.magick.convert('-quiet', filepath, '-quality', '0', aip_image_path, _bg=True)
            folder_data = get_folder_data(file_parts['folder_id'])
            arrangement_parts = get_arrangement_parts(folder_data)
            print(json.dumps(arrangement_parts, sort_keys=True, indent=4))
            xmp_dc = get_xmp_dc_metadata(arrangement_parts, file_parts, folder_data, collection_json)
            print(json.dumps(xmp_dc, sort_keys=True, indent=4))
            aip_image_conversion.wait()
            write_xmp_metadata(aip_image_path, xmp_dc)
            # NOTE: unsure how to run with _bg=True from a function
            aip_image_signature = sh.cut(sh.sha512sum(sh.magick.stream('-quiet', '-map', 'rgb', '-storage-type', 'short', aip_image_path, '-', _piped=True, _bg=True), _bg=True), '-d', ' ', '-f', '1', _bg=True)
            aip_image_data = get_aip_image_data(aip_image_path)
            print(json.dumps(aip_image_data, sort_keys=True, indent=4))
            sip_image_signature.wait()
            aip_image_signature.wait()
            # verify image signatures match
            if aip_image_signature == sip_image_signature:
                pass
            else:
                print('❌  image signatures did not match: ' + file_parts['image_id'])
                continue
            # begin s3 processing
            aip_image_key = get_s3_aip_image_key(arrangement_parts, file_parts)
            print(aip_image_key)
            put_s3_object_response = put_s3_object(AIP_BUCKET, aip_image_key, aip_image_data)
            print(json.dumps(put_s3_object_response, sort_keys=True, indent=4))
            digital_object_component = create_digital_object_component(folder_data, file_parts, AIP_BUCKET, aip_image_key, aip_image_data)
            print(json.dumps(digital_object_component, sort_keys=True, indent=4))
            digital_object_component_post_response = post_digital_object_component(digital_object_component)
            print(json.dumps(json.loads(digital_object_component_post_response.text), sort_keys=True, indent=4))
